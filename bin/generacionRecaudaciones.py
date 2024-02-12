import sys
import requests
#from smbprotocol import open_file, register_session
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
cuatrimestre = int(sys.argv[2])
user = int(sys.argv[3])
area = int(sys.argv[4])

storage_client = storage.Client()

"""client = secretmanager.SecretManagerServiceClient()
smb_host = client.access_secret_version(name="SFTP_HOST").payload.data.decode("UTF-8")
smb_port = client.access_secret_version(name="SFTP_PORT").payload.data.decode("UTF-8")
smb_user = client.access_secret_version(name="SFTP_USERNAME").payload.data.decode("UTF-8")
smb_pass = client.access_secret_version(name="SFTP_PASSWORD").payload.data.decode("UTF-8")
smb_remote_file_path = client.access_secret_version(name="SFTP_CARPETA_DESTINO").payload.data.decode("UTF-8")
"""

#register_session(smb_host, username=smb_user, password=smb_pass)


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


def process_dataframe(df, identifier):
    return df.rdd.flatMap(lambda row: [([identifier] + [row[i] for i in range(len(row))])]).collect()


def str_to_gcs(data, name):
    blob = bucket.blob(f"correspondencia/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")
    print(F"SE CREO EL ARCHIVO DE correspondencia/{name}")


def get_months(cuatrimestre):
    year = str(cuatrimestre)[:4]
    cuatrimestre_int = int(str(cuatrimestre)[-2:])
    last_month = (cuatrimestre_int * 3) + cuatrimestre_int
    first_month = last_month - 3
    months = [int(year + "0" + str(month)) for month in range(first_month, last_month + 1)]
    return months


"""def upload_file_to_smb(remote_file_path, filename, data):
    with open_file(f"{remote_file_path}/{filename}", "w") as fd:
        fd.write(data)"""


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
        months = get_months(cuatrimestre)
        full_months = months + (months)
        body_message = ""

        for month in full_months:
            print(f"GENERACION DE RECAUDACIONES PARA EL MES {month}")

            query_indicador = """
                        SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR"
                        WHERE "FTB_IMPRESION" = TRUE
                        AND "FTB_ENVIO" = FALSE
                        """
            read_table_insert_temp_view(
                configure_postgres_spark,
                query_indicador,
                "indicador"
            )

            query_general = """
                                                    SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_GENERAL"
                                                    """
            read_table_insert_temp_view(
                configure_postgres_spark,
                query_general,
                "general"
            )

            df_general = spark.sql("SELECT * FROM general")
            df_indicador = spark.sql("SELECT * FROM indicador")

            if type(month) == list:
                df_general = (
                    df_general.filter(f.col("FCN_ID_PERIODO").isin(month))
                )
            else:
                df_general = (
                    df_general.filter(f.col("FCN_ID_PERIODO") == month)
                )

            df_general = (
                df_general.withColumn("FCN_NUMERO_CUENTA", f.lpad(f.col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))
            )

            df_general = df_general.join(df_indicador, df_general.FCN_ID_EDOCTA == df_indicador.FCN_CUENTA,
                                         'left')
            if df_general.count() > 0:

                df_general.cache()

                query_anverso = """
                                                    SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_ANVERSO"
                                                    """
                read_table_insert_temp_view(
                    configure_postgres_spark,
                    query_anverso,
                    "anverso"
                )

                df_anverso = spark.sql("SELECT * FROM anverso")

                df_anverso = (df_anverso
                              .withColumnRenamed("FCN_NUMERO_CUENTA", "FCN_NUMERO_CUENTA_ANVERSO")
                              .withColumn("FCN_NUMERO_CUENTA_ANVERSO",
                                          f.lpad(f.col("FCN_NUMERO_CUENTA_ANVERSO").cast("string"), 10, "0"))
                              )

                df_anverso_general = df_anverso.join(df_general, "FCN_ID_EDOCTA").fillna(value="")

                df_anverso_general = (
                    df_anverso_general
                    .withColumn("FTC_CONCEPTO_NEGOCIO",
                                f.when(df_anverso_general.FTC_CONCEPTO_NEGOCIO == "NO APLICA", "")
                                .otherwise(df_anverso_general.FTC_CONCEPTO_NEGOCIO))
                )

                df_general = (
                    df_general.withColumn("FTD_FECHA_GRAL_INICIO",
                                          f.to_date(f.col("FTD_FECHA_GRAL_INICIO"), "ddMMyyyy"))
                    .withColumn("FTD_FECHA_GRAL_FIN", f.to_date(f.col("FTD_FECHA_GRAL_FIN"), "ddMMyyyy"))
                    .withColumn("FTD_FECHA_CORTE", f.to_date(f.col("FTD_FECHA_CORTE"), "ddMMyyyy"))
                )
                unique_clients = df_anverso_general.select("FCN_ID_EDOCTA").distinct()
                df_general = df_general.join(unique_clients, on="FCN_ID_EDOCTA", how="inner")

                df_general = (
                    df_general.select("FCN_NUMERO_CUENTA", "FCN_FOLIO", "FTC_NOMBRE_COMPLETO", "FTC_CALLE_NUMERO",
                                      "FTC_COLONIA", "FTC_DELEGACION", "FTN_CP", "FTC_ENTIDAD_FEDERATIVA", "FTC_RFC",
                                      "FTC_NSS", "FTC_CURP",
                                      f.date_format("FTD_FECHA_GRAL_INICIO", "ddMMyyyy").alias("FTD_FECHA_GRAL_INICIO"),
                                      f.date_format("FTD_FECHA_GRAL_FIN", "ddMMyyyy").alias("FTD_FECHA_GRAL_FIN"),
                                      "FTN_ID_FORMATO",
                                      "FTN_ID_SIEFORE",
                                      f.date_format("FTD_FECHA_CORTE", "ddMMyyyy").alias("FTD_FECHA_CORTE"),
                                      f.col("FTN_SALDO_SUBTOTAL").cast("decimal(16, 2)"),
                                      f.col("FTN_SALDO_TOTAL").cast("decimal(16, 2)"),
                                      f.col("FTN_PENSION_MENSUAL").cast("decimal(16, 2)"), "FTC_TIPO_TRABAJADOR",
                                      "FTC_FORMATO"
                                      )
                )

                df_anverso_aho = (
                    df_anverso_general.filter(
                        f.col("FTC_SECCION").isin(["AHO", "PEN"]))
                    .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_CONCEPTO_NEGOCIO",
                            f.col("FTN_SALDO_ANTERIOR").cast("decimal(16, 2)"),
                            f.col("FTF_APORTACION").cast("decimal(16, 2)"),
                            f.col("FTN_RETIRO").cast("decimal(16, 2)"),
                            f.col("FTN_RENDIMIENTO").cast("decimal(16, 2)"),
                            f.col("FTN_COMISION").cast("decimal(16, 2)"),
                            f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))

                )

                df_anverso_bon = (
                    df_anverso_general.filter(f.col("FTC_SECCION") == "BON")
                    .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_CONCEPTO_NEGOCIO",
                            f.col("FTN_VALOR_ACTUAL_UDI").cast("decimal(16, 2)"),
                            f.col("FTN_VALOR_NOMINAL_UDI").cast("decimal(16, 2)"),
                            f.col("FTN_VALOR_ACTUAL_PESO").cast("decimal(16, 2)"),
                            f.col("FTN_VALOR_NOMINAL_PESO").cast("decimal(16, 2)"))

                )

                df_anverso_sdo = (
                    df_anverso_general.filter((f.col("FTC_SECCION") == "SDO"))
                    .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_GRUPO_CONCEPTO", "FTC_CONCEPTO_NEGOCIO",
                            f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))

                )

                general_count = df_general.count()
                ahorro_count = df_anverso_aho.count()
                bono_count = df_anverso_bon.count()
                saldo_count = df_anverso_sdo.count()
                total_count = sum([general_count, ahorro_count, bono_count, saldo_count])
                print("COUNTERS: ", general_count, ahorro_count, bono_count, saldo_count, total_count)

                limit = 500000
                num_parts_anverso = math.ceil(total_count / limit)

                processed_data = []
                processed_data += process_dataframe(df_general.fillna("").fillna(0), 1)
                processed_data += process_dataframe(df_anverso_aho.fillna("").fillna(0), 2)
                processed_data += process_dataframe(df_anverso_bon.fillna("").fillna(0), 3)
                processed_data += process_dataframe(df_anverso_sdo.fillna("").fillna(0), 4)
                print("DATOS PROCESADOS ANVERSO")

                for part in range(1, num_parts_anverso + 1):
                    if len(processed_data) < limit:
                        processed_data_part = processed_data
                        res = "\n".join("|".join(str(item) for item in row) for row in processed_data_part)
                        final_row = f"\n5|{general_count}|{ahorro_count}|{bono_count}|{saldo_count}|{total_count}"
                        res = res + final_row
                    else:
                        processed_data_part = processed_data[:limit - 1]
                        res = "\n".join("|".join(str(item) for item in row) for row in processed_data_part)
                        del processed_data_part[:limit - 1]

                    name_anverso = f"recaudacion_anverso_mensual_{str(month)[:4]}_{str(month)[-2:]}_{part}-{num_parts_anverso}.txt" if type(
                        month) == int else f"recaudacion_anverso_cuatrimestral_{str(cuatrimestre)[:4]}_{str(cuatrimestre)[-2:]}_{part}-{num_parts_anverso}.txt"
                    str_to_gcs(res, name_anverso)
                    # upload_file_to_smb(smb_remote_file_path, name_anverso, res)
                    body_message += f"Se generó el archivo de {name_anverso} con un total de {len(processed_data_part)} registros\n"

                query_reverso = """
                    SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_REVERSO"
                                    """
                read_table_insert_temp_view(
                    configure_postgres_spark,
                    query_reverso,
                    "reverso"
                )

                df_edo_reverso = spark.sql("SELECT * FROM reverso")

                df_reverso_general = df_edo_reverso.join(df_general, "FCN_NUMERO_CUENTA")
                df_reverso_general = (
                    df_reverso_general.select("FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION",
                                              f.date_format("FTD_FECHA_MOVIMIENTO", "ddMMyyyy").alias(
                                                  "FTD_FECHA_MOVIMIENTO"),
                                              "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO",
                                              f.col("FTN_SALARIO_BASE").cast("decimal(16, 2)"),
                                              f.col("FTN_MONTO").cast("decimal(16, 2)"))
                )
                ret = df_reverso_general.filter(f.col("FTC_SECCION") == "RET").count()
                vol = df_reverso_general.filter(f.col("FTC_SECCION") == "VOL").count()
                viv = df_reverso_general.filter(f.col("FTC_SECCION") == "VIV").count()
                total = df_reverso_general.count()

                num_parts_reverso = math.ceil(total / limit)

                reverso_data = process_dataframe(df_reverso_general.fillna("").fillna(0), 1)
                print("DATOS PROCESADOS REVERSO")

                for part in range(1, num_parts_reverso + 1):
                    if len(reverso_data) < limit:
                        reverso_data_part = reverso_data
                        res = "\n".join("|".join(str(item) for item in row) for row in reverso_data_part)
                        reverso_final_row = f"\n2|{ret}|{vol}|{viv}|{total}"
                        res = res + reverso_final_row
                    else:
                        reverso_data_part = reverso_data[:limit]
                        res = "\n".join("|".join(str(item) for item in row) for row in reverso_data_part)
                        del reverso_data[:limit - 1]

                    name_reverso = f"recaudacion_reverso_mensual_{str(month)[:4]}_{str(month)[-2:]}_{part}-{num_parts_reverso}.txt" if type(
                        month) == int else f"recaudacion_reverso_cuatrimestral_{str(cuatrimestre)[:4]}_{str(cuatrimestre)[-2:]}_{part}-{num_parts_reverso}.txt"
                    str_to_gcs(res, name_reverso)
                    # upload_file_to_smb(smb_remote_file_path, name_reverso, res)
                    body_message += f"Se generó el archivo de {name_reverso} con un total de {len(reverso_data_part)} registros\n"

            send_email(email_url, "", "Generacion de los archivos de recaudacion", body_message)

        """notify(
            postgres,
            f"Generacion de archivos Recaudaciones",
            phase,
            area,
            term=term_id,
            message=f"Se han exportado recaudaciones para el cuatrimestre {cuatrimestre}",
            aprobar=False,
            descarga=False
        )"""
