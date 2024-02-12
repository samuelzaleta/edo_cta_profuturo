from smbprotocol import open_file, register_session
import requests
import sys
import math
from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, configure_postgres_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from google.cloud import storage, secretmanager
import pyspark.sql.functions as f

spark = _get_spark_session()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

storage_client = storage.Client()

client = secretmanager.SecretManagerServiceClient()
smb_host = client.access_secret_version(name="SFTP_HOST").payload.data.decode("UTF-8")
smb_port = client.access_secret_version(name="SFTP_PORT").payload.data.decode("UTF-8")
smb_user = client.access_secret_version(name="SFTP_USERNAME").payload.data.decode("UTF-8")
smb_pass = client.access_secret_version(name="SFTP_PASSWORD").payload.data.decode("UTF-8")
smb_remote_file_path = client.access_secret_version(name="SFTP_CARPETA_DESTINO").payload.data.decode("UTF-8")


def get_buckets():
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name.startswith("edo_cuenta_profuturo"):
            return bucket.name


bucket_name = get_buckets()
bucket = storage_client.get_bucket(bucket_name)

if "dev" in bucket_name:
    email_url = "https://procesos-api-service-dev-e46ynxyutq-uk.a.run.app/procesos/email/send"
elif "qa" in bucket_name:
    email_url = "https://procesos-api-service-qa-2ky75pylsa-uk.a.run.app/procesos/email/send"
else:
    email_url = "https://procesos-api-service-h3uy3grcoq-uk.a.run.app/procesos/email/send"


def str_to_gcs(data, name):
    blob = bucket.blob(f"correspondencia/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")


def upload_file_to_smb(remote_file_path, filename, data):
    with open_file(f"{remote_file_path}/{filename}", "w") as fd:
        fd.write(data)


def send_email(url, receiver, subject, text):
    data = {
        "to": f"{receiver};",
        "subject": subject,
        "text": text
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.request("POST", url, data=data, headers=headers)

    if response.status_code == 200:
        print("Correo enviado con éxito!")
    else:
        print(f"Error al enviar correo: {response.status_code}")


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        body_message = ""

        query_retiro_general = """
                            SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_RETIRO_GENERAL"
                                            """
        read_table_insert_temp_view(
            configure_postgres_spark,
            query_retiro_general,
            "retiros_general"
        )

        retiros_general = spark.sql("SELECT * FROM retiros_general")

        retiros_general = (
            retiros_general.filter(
                f.col("FCN_ID_PERIODO") == term_id)
        )

        query_retiro = """
                                    SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_RETIRO"
                                                    """
        read_table_insert_temp_view(
            configure_postgres_spark,
            query_retiro,
            "retiros"
        )

        retiros = spark.sql("SELECT * FROM retiros")
        retiros = (
            retiros.filter(f.col("FCN_ID_PERIODO") == term_id).withColumn(
                "FTD_FECHA_EMISION_2", f.date_format("FTD_FECHA_EMISION", "yyyyMMdd"))
        )

        df = (
            retiros_general.join(retiros, 'FCN_NUMERO_CUENTA').select(
                f.date_format("FTD_FECHA_EMISION", "yyyyMMdd").alias("FTD_FECHA_EMISION"),
                "FTC_NOMBRE", "FTC_CALLE_NUMERO", "FTC_COLONIA",
                "FTC_MUNICIPIO", "FTC_CP",
                "FTC_ENTIDAD", "FTC_CURP", "FTC_RFC", "FTC_NSS", f.col("FTN_SDO_INI_AHO_RET").cast("decimal(16, 2)"),
                f.col("FTN_SDO_INI_AHO_VIV").cast("decimal(16, 2)"),
                f.col("FTN_SDO_TRA_AHO_RET").cast("decimal(16, 2)"),
                f.col("FTN_SDO_TRA_AHO_VIV").cast("decimal(16, 2)"),
                f.col("FTN_SDO_REM_AHO_RET").cast("decimal(16, 2)"),
                f.col("FTN_SDO_REM_AHO_VIV").cast("decimal(16, 2)"),
                "FTC_LEY_PENSION", "FTC_REGIMEN", "FTC_SEGURO", "FTC_TIPO_PENSION", "FTC_FON_ENTIDAD",
                "FTC_FON_NUMERO_POLIZA", f.col("FTN_FON_MONTO_TRANSF").cast("decimal(16, 2)"),
                f.date_format("FTD_FON_FECHA_TRANSF", "yyyyMMdd").alias("FTD_FON_FECHA_TRANSF"),
                f.col("TFN_FON_RETENCION_ISR").cast("decimal(16, 2)"), "FTC_AFO_ENTIDAD", "FTC_AFO_MEDIO_PAGO",
                f.col("FTC_AFO_RECURSOS_ENTREGA").cast("decimal(16, 2)"),
                f.date_format("FTD_AFO_FECHA_ENTREGA", "yyyyMMdd").alias("FTD_AFO_FECHA_ENTREGA"),
                f.col("FTC_AFO_RETENCION_ISR").cast("decimal(16, 2)"), "FTC_INFNVT_ENTIDAD",
                "FTC_INFNVT_CTA_BANCARIA", f.col("FTC_INFNVT_RECURSOS_ENTREGA").cast("decimal(16, 2)"),
                f.col("FTC_INFNVT_FECHA_ENTREGA").cast("string"),
                f.col("FTC_INFNVT_RETENCION_ISR").cast("decimal(16, 2)"),
                "FTD_FECHA_EMISION_2",
                retiros.FTC_FECHA_INICIO_PENSION.alias("FTC_FECHA_INICIO_PENSION"),
                f.col("FTN_PENSION_INSTITUTO_SEG"),
                f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))
        )
        df = df.fillna("").fillna(0)
        df = (
            df.withColumn("FTD_FECHA_EMISION", f.rpad(f.col("FTD_FECHA_EMISION").cast("string"), 8, " "))
            .withColumn("FTC_NOMBRE", f.rpad(f.col("FTC_NOMBRE"), 60, " "))
            .withColumn("FTC_CALLE_NUMERO", f.rpad(f.col("FTC_CALLE_NUMERO"), 60, " "))
            .withColumn("FTC_COLONIA", f.rpad(f.col("FTC_COLONIA"), 30, " "))
            .withColumn("FTC_MUNICIPIO", f.rpad(f.col("FTC_MUNICIPIO"), 60, " "))
            .withColumn("FTC_CP", f.lpad(f.translate(f.col("FTC_CP").cast("string"), ".", ""), 5, "0"))
            .withColumn("FTC_ENTIDAD", f.rpad(f.col("FTC_ENTIDAD"), 30, " "))
            .withColumn("FTC_CURP", f.rpad(f.col("FTC_CURP"), 18, " "))
            .withColumn("FTC_RFC", f.rpad(f.col("FTC_RFC"), 13, " "))
            .withColumn("FTC_NSS", f.rpad(f.translate(f.col("FTC_NSS").cast("string"), ".", ""), 11, "0"))
            .withColumn("FTN_SDO_INI_AHO_RET",
                        f.lpad(f.translate(f.col("FTN_SDO_INI_AHO_RET").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_INI_AHO_VIV",
                        f.lpad(f.translate(f.col("FTN_SDO_INI_AHO_VIV").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_TRA_AHO_RET",
                        f.lpad(f.translate(f.col("FTN_SDO_TRA_AHO_RET").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_TRA_AHO_VIV",
                        f.lpad(f.translate(f.col("FTN_SDO_TRA_AHO_VIV").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_REM_AHO_RET",
                        f.lpad(f.translate(f.col("FTN_SDO_REM_AHO_RET").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_REM_AHO_VIV",
                        f.lpad(f.translate(f.col("FTN_SDO_REM_AHO_VIV").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_LEY_PENSION", f.rpad(f.col("FTC_LEY_PENSION"), 6, " "))
            .withColumn("FTC_REGIMEN", f.rpad(f.col("FTC_REGIMEN"), 2, " "))
            .withColumn("FTC_SEGURO", f.rpad(f.col("FTC_SEGURO"), 2, " "))
            .withColumn("FTC_TIPO_PENSION", f.rpad(f.col("FTC_TIPO_PENSION"), 2, " "))
            .withColumn("FTC_FON_ENTIDAD", f.rpad(f.col("FTC_FON_ENTIDAD"), 16, " "))
            .withColumn("FTC_FON_NUMERO_POLIZA", f.rpad(f.col("FTC_FON_NUMERO_POLIZA"), 9, " "))
            .withColumn("FTN_FON_MONTO_TRANSF",
                        f.lpad(f.translate(f.col("FTN_FON_MONTO_TRANSF").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTD_FON_FECHA_TRANSF", f.rpad(f.col("FTD_FON_FECHA_TRANSF").cast("string"), 8, " "))
            .withColumn("TFN_FON_RETENCION_ISR",
                        f.lpad(f.translate(f.col("TFN_FON_RETENCION_ISR").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_AFO_ENTIDAD", f.rpad(f.col("FTC_AFO_ENTIDAD"), 20, " "))
            .withColumn("FTC_AFO_MEDIO_PAGO", f.rpad(f.col("FTC_AFO_MEDIO_PAGO"), 15, " "))
            .withColumn("FTC_AFO_RECURSOS_ENTREGA",
                        f.lpad(f.translate(f.col("FTC_AFO_RECURSOS_ENTREGA").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTD_AFO_FECHA_ENTREGA", f.rpad(f.col("FTD_AFO_FECHA_ENTREGA"), 8, " "))
            .withColumn("FTC_AFO_RETENCION_ISR",
                        f.lpad(f.translate(f.col("FTC_AFO_RETENCION_ISR").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_INFNVT_ENTIDAD", f.rpad(f.col("FTC_INFNVT_ENTIDAD"), 14, " "))
            .withColumn("FTC_INFNVT_CTA_BANCARIA",
                        f.lpad(f.translate(f.col("FTC_INFNVT_CTA_BANCARIA").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_INFNVT_RECURSOS_ENTREGA",
                        f.lpad(f.translate(f.col("FTC_INFNVT_RECURSOS_ENTREGA").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_INFNVT_FECHA_ENTREGA", f.rpad(f.col("FTC_INFNVT_FECHA_ENTREGA"), 8, " "))
            .withColumn("FTC_INFNVT_RETENCION_ISR",
                        f.lpad(f.translate(f.col("FTC_INFNVT_RETENCION_ISR").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTD_FECHA_EMISION_2", f.rpad(f.col("FTD_FECHA_EMISION_2").cast("string"), 8, " "))
            .withColumn("FTC_FECHA_INICIO_PENSION", f.rpad(f.col("FTC_FECHA_INICIO_PENSION").cast("string"), 8, " "))
            .withColumn("FTN_PENSION_INSTITUTO_SEG", f.rpad(f.col("FTN_PENSION_INSTITUTO_SEG"), 13, " "))
            .withColumn("FTN_SALDO_FINAL",
                        f.lpad(f.translate(f.col("FTN_SALDO_FINAL").cast("string"), ".", ""), 10, "0"))
        )

        regimenes = ["73", "97"]
        for regimen in regimenes:
            df_regimen = df.filter(f.col("FTC_REGIMEN") == regimen)
            total = df_regimen.count()
            limit = 500000
            num_parts = math.ceil(total / limit)
            processed_data = df_regimen.rdd.flatMap(lambda row: [([row[i] for i in range(len(row))])]).collect()

            for part in range(1, num_parts + 1):
                if len(processed_data) < limit:
                    processed_data_part = processed_data[:len(processed_data)]
                else:
                    processed_data_part = processed_data[:limit - 1]
                    del processed_data[:limit - 1]

                res = "\n".join("".join(str(item) for item in row) for row in processed_data_part)
                name = f"retiros_mensual_{regimen}_{str(term_id)[:4]}_{str(term_id)[-2:]}_{part}-{num_parts}.txt"
                str_to_gcs(res, name)
                upload_file_to_smb(smb_remote_file_path, name, res)
                body_message += f"Se generó el archivo de {name} con un total de {len(processed_data_part)} registros\n"
                processed_data = [i for i in processed_data if i not in processed_data_part]

        send_email(email_url, "", "Generacion de los archivos de retiros", body_message)

        """notify(
            postgres,
            f"Generacion de archivos Retiros",
            phase,
            area,
            term=term_id,
            message=f"Se han exportado retiros para el periodo {term_id}",
            aprobar=False,
            descarga=False
        )"""
