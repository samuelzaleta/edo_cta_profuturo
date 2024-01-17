import random
import paramiko
import smtplib
import sys
from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool
from profuturo.extraction import extract_terms, _get_spark_session
from profuturo.reporters import HtmlReporter
from google.cloud import storage, bigquery
import pyspark.sql.functions as f

spark = _get_spark_session()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
cuatrimestre = int(sys.argv[2])
user = int(sys.argv[3])
area = int(sys.argv[4])

storage_client = storage.Client()
bigquery_client = bigquery.Client()
bigquery_project = bigquery_client.project


def get_buckets():
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name.startswith("edo_cuenta_profuturo"):
            return bucket.name


bucket = storage_client.get_bucket(get_buckets())


def extract_bigquery(table):
    df = spark.read.format('bigquery') \
        .option('table', f'{bigquery_project}:{table}') \
        .load()
    print(f"SE EXTRAIDO EXITOSAMENTE {table}")
    return df


def get_months(cuatrimestre):
    year = str(cuatrimestre)[:4]
    cuatrimestre_int = int(str(cuatrimestre)[-2:])
    last_month = (cuatrimestre_int * 3) + cuatrimestre_int
    first_month = last_month - 3
    months = [int(year + "0" + str(month)) for month in range(first_month, last_month + 1)]
    return months


def str_to_gcs(data, name):
    blob = bucket.blob(f"correspondencia/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")


def upload_file_to_sftp(hostname, username, password, local_file_path, remote_file_path, data):
    with open(f"{local_file_path}", "w") as file:
        file.write(data)

    try:
        transport = paramiko.Transport((hostname, 22))
        transport.connect(username=username, password=password)

        sftp = paramiko.SFTPClient.from_transport(transport)

        sftp.put(local_file_path, remote_file_path)

        print(f"File uploaded successfully to {remote_file_path} on SFTP server {hostname}")

    except Exception as e:
        print(f"An error occurred during SFTP upload: {e}")

    finally:
        sftp.close()
        transport.close()


def send_email(host, port, from_address, to_address, subject, body):
    username = "Profuturo"
    password = ""
    server = smtplib.SMTP(host, port)
    server.login(username, password)
    message = "Subject: {}\n\n{}".format(subject, body)
    server.sendmail(from_address, to_address, message)
    server.quit()


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        months = get_months(cuatrimestre)
        full_months = months + [months]
        body_message = ""

        for month in full_months:

            if type(month) == list:
                retiros_general = (
                    extract_bigquery('ESTADO_CUENTA.TTEDOCTA_RETIRO_GENERAL').filter(
                        f.col("FCN_ID_PERIODO").isin(month))
                )
                retiros = (
                    extract_bigquery('ESTADO_CUENTA.TTEDOCTA_RETIRO').filter(f.col("FCN_ID_PERIODO").isin(month))
                )
            else:
                retiros_general = (
                    extract_bigquery('ESTADO_CUENTA.TTEDOCTA_RETIRO_GENERAL').filter(
                        f.col("FCN_ID_PERIODO") == month)
                )
                retiros = (
                    extract_bigquery('ESTADO_CUENTA.TTEDOCTA_RETIRO').filter(f.col("FCN_ID_PERIODO") == month)
                )

            retiros_general = (
                retiros_general
                .select("FCN_NUMERO_CUENTA", "FTC_NOMBRE", "FTC_CALLE_NUMERO", "FTC_COLONIA", "FTC_MUNICIPIO", "FTC_CP",
                        "FTC_ENTIDAD", "FTC_CURP", "FTC_RFC", "FTC_NSS")
            )

            retiros = (
                retiros
                .select("FCN_NUMERO_CUENTA", f.date_format("FTD_FECHA_EMISION", "ddMMyyyy").alias("FTD_FECHA_EMISION"),
                        f.col("FTN_SDO_INI_AHO_RET").cast("decimal(16, 2)"),
                        f.col("FTN_SDO_INI_AHO_VIV").cast("decimal(16, 2)"),
                        f.col("FTN_SDO_TRA_AHO_RET").cast("decimal(16, 2)"),
                        f.col("FTN_SDO_TRA_AHO_VIV").cast("decimal(16, 2)"),
                        f.col("FTN_SDO_REM_AHO_RET").cast("decimal(16, 2)"),
                        f.col("FTN_SDO_REM_AHO_VIV").cast("decimal(16, 2)"),
                        "FTC_LEY_PENSION", "FTC_REGIMEN", "FTC_SEGURO", "FTC_TIPO_PENSION", "FTC_FON_ENTIDAD",
                        "FTC_FON_NUMERO_POLIZA", f.col("FTN_FON_MONTO_TRANSF").cast("decimal(16, 2)"),
                        f.date_format("FTD_FON_FECHA_TRANSF", "ddMMyyyy").alias("FTD_FON_FECHA_TRANSF"),
                        f.col("TFN_FON_RETENCION_ISR").cast("decimal(16, 2)"), "FTC_AFO_ENTIDAD", "FTC_AFO_MEDIO_PAGO",
                        "FTC_AFO_RECURSOS_ENTREGA",
                        f.date_format("FTD_AFO_FECHA_ENTREGA", "ddMMyyyy").alias("FTD_AFO_FECHA_ENTREGA"),
                        "FTC_AFO_RETENCION_ISR", "FTC_INFNVT_ENTIDAD",
                        "FTC_INFNVT_CTA_BANCARIA", "FTC_INFNVT_RECURSOS_ENTREGA", "FTC_INFNVT_FECHA_ENTREGA",
                        "FTC_INFNVT_RETENCION_ISR",
                        f.date_format("FTD_FECHA_EMISION", "ddMMyyyy").alias("FTD_FECHA_EMISION_2"),
                        f.date_format("FTD_FECHA_INICIO_PENSION", "ddMMyyyy").alias("FTD_FECHA_INICIO_PENSION"),
                        f.col("FTN_PENSION_INSTITUTO_SEG").cast("decimal(16, 2)"),
                        f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))
            )

            df = retiros_general.join(retiros, 'FCN_NUMERO_CUENTA').drop(retiros['FCN_NUMERO_CUENTA'])
            total = df.count()
            processed_data = df.rdd.flatMap(lambda row: [([row[i] for i in range(len(row))])]).collect()
            res = "\n".join("|".join(str(item) for item in row) for row in processed_data)

            name = f"retiros_mensual_{str(month)[:4]}_{str(month)[-2:]}_1-1.txt" if type(
                month) == int else f"retiros_cuatrimestral_{str(cuatrimestre)[:4]}_{str(cuatrimestre)[-2:]}_1-1.txt"

            str_to_gcs(res, name)

            upload_file_to_sftp("", "", "", name, "", res)

            body_message += f"Se generó el archivo de {name} con un total de {total} registros\n"

        send_email(
            host="cluster4.us.messagelabs.com",
            port=25,
            from_address="sender@example.com",
            to_address="alfredo.guerra@profuturo.com.mx",
            subject="Generacion de los archivos de recaudacion",
            body=body_message
        )
        
        notify(
            postgres,
            f"Generacion de archivos Retiros",
            phase,
            area,
            term=term_id,
            message=f"Se han exportado recaudaciones para el cuatrimestre {cuatrimestre}",
            aprobar=False,
            descarga=False
        )

