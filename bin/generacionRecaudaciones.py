import sys
import smtplib
import paramiko
from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool
from profuturo.extraction import extract_terms, _get_spark_session
from profuturo.reporters import HtmlReporter
from google.cloud import storage, bigquery, secretmanager
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

client = secretmanager.SecretManagerServiceClient()
"""smtp_host = client.access_secret_version(name="SMTP_HOST").payload.data.decode("UTF-8")
smtp_port = client.access_secret_version(name="SMTP_PORT").payload.data.decode("UTF-8")
smtp_user = client.access_secret_version(name="SMTP_ADDRESS_SENDER").payload.data.decode("UTF-8")
smtp_pass = client.access_secret_version(name="SMTP_PASSWORD_SENDER").payload.data.decode("UTF-8")
sftp_host = client.access_secret_version(name="SFTP_HOST").payload.data.decode("UTF-8")
sftp_port = client.access_secret_version(name="SFTP_PORT").payload.data.decode("UTF-8")
sftp_user = client.access_secret_version(name="SFTP_USERNAME").payload.data.decode("UTF-8")
sftp_pass = client.access_secret_version(name="SFTP_PASSWORD").payload.data.decode("UTF-8")
sftp_remote_file_path = client.access_secret_version(name="SFTP_CARPETA_DESTINO").payload.data.decode("UTF-8")"""


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


def process_dataframe(df, identifier):
    return df.rdd.flatMap(lambda row: [([identifier] + [row[i] for i in range(len(row))])]).collect()


def str_to_gcs(data, name):
    blob = bucket.blob(f"correspondencia/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")


def get_months(cuatrimestre):
    year = str(cuatrimestre)[:4]
    cuatrimestre_int = int(str(cuatrimestre)[-2:])
    last_month = (cuatrimestre_int * 3) + cuatrimestre_int
    first_month = last_month - 3
    months = [int(year + "0" + str(month)) for month in range(first_month, last_month + 1)]
    return months


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


def send_email(host, port, username, password, from_address, to_address, subject, body):
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
            df_indicador = (
                extract_bigquery('ESTADO_CUENTA.TTEDOCTA_CLIENTE_INDICADOR')
                .filter((f.col("FTB_IMPRESION") == True) & (f.col("FTB_ENVIO") == False))
            )

            if type(month) == list:
                df_general = (
                    extract_bigquery('ESTADO_CUENTA.TTEDOCTA_GENERAL').filter(f.col("FCN_ID_PERIODO").isin(month))
                )
            else:
                df_general = (
                    extract_bigquery('ESTADO_CUENTA.TTEDOCTA_GENERAL').filter(f.col("FCN_ID_PERIODO") == month)
                )

            df_general = (
                df_general.withColumn("FCN_NUMERO_CUENTA", f.lpad(f.col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))
            )

            df_general = df_general.join(df_indicador, df_general.FCN_ID_EDOCTA == df_indicador.FCN_CUENTA,
                                         'left')

            df_anverso = (
                extract_bigquery('ESTADO_CUENTA.TTEDOCTA_ANVERSO')
                .withColumnRenamed("FCN_NUMERO_CUENTA", "FCN_NUMERO_CUENTA_ANVERSO")
                .withColumn("FCN_NUMERO_CUENTA_ANVERSO",
                            f.lpad(f.col("FCN_NUMERO_CUENTA_ANVERSO").cast("string"), 10, "0"))
            )

            df_anverso_general = df_anverso.join(df_general, "FCN_ID_EDOCTA").fillna(value="")

            df_anverso_general = (
                df_anverso_general
                .withColumn("FTC_CONCEPTO_NEGOCIO", f.when(df_anverso_general.FTC_CONCEPTO_NEGOCIO == "NO APLICA", "")
                            .otherwise(df_anverso_general.FTC_CONCEPTO_NEGOCIO))
            )

            df_general = (
                df_general.withColumn("FTD_FECHA_GRAL_INICIO", f.to_date(f.col("FTD_FECHA_GRAL_INICIO"), "ddMMyyyy"))
                .withColumn("FTD_FECHA_GRAL_FIN", f.to_date(f.col("FTD_FECHA_GRAL_FIN"), "ddMMyyyy"))
                .withColumn("FTD_FECHA_CORTE", f.to_date(f.col("FTD_FECHA_CORTE"), "ddMMyyyy"))
            )

            unique_clients = df_anverso_general.select("FCN_ID_EDOCTA").distinct().rdd.collect()
            clients = [row.FCN_ID_EDOCTA for row in unique_clients]

            df_general = (
                df_general.filter(f.col("FCN_ID_EDOCTA").isin(clients))
                .select("FCN_NUMERO_CUENTA", "FCN_FOLIO", "FTC_NOMBRE_COMPLETO", "FTC_CALLE_NUMERO",
                        "FTC_COLONIA", "FTC_DELEGACION", "FTN_CP", "FTC_ENTIDAD_FEDERATIVA", "FTC_RFC",
                        "FTC_NSS", "FTC_CURP",
                        f.date_format("FTD_FECHA_GRAL_INICIO", "ddMMyyyy").alias("FTD_FECHA_GRAL_INICIO"),
                        f.date_format("FTD_FECHA_GRAL_FIN", "ddMMyyyy").alias("FTD_FECHA_GRAL_FIN"),
                        "FTN_ID_FORMATO",
                        "FTN_ID_SIEFORE", f.date_format("FTD_FECHA_CORTE", "ddMMyyyy").alias("FTD_FECHA_CORTE"),
                        f.col("FTN_SALDO_SUBTOTAL").cast("decimal(16, 2)"),
                        f.col("FTN_SALDO_TOTAL").cast("decimal(16, 2)"),
                        f.col("FTN_PENSION_MENSUAL").cast("decimal(16, 2)"), "FTC_TIPO_TRABAJADOR",
                        "FTC_FORMATO"
                        )
            )

            df_anverso_aho = (
                df_anverso_general.filter(
                    (f.col("FTC_SECCION").isin(["AHO", "PEN"])) & (f.col("FCN_ID_EDOCTA").isin(clients)))
                .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_CONCEPTO_NEGOCIO",
                        f.col("FTN_SALDO_ANTERIOR").cast("decimal(16, 2)"),
                        f.col("FTF_APORTACION").cast("decimal(16, 2)"),
                        f.col("FTN_RETIRO").cast("decimal(16, 2)"),
                        f.col("FTN_RENDIMIENTO").cast("decimal(16, 2)"),
                        f.col("FTN_COMISION").cast("decimal(16, 2)"),
                        f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))

            )

            df_anverso_bon = (
                df_anverso_general.filter((f.col("FTC_SECCION") == "BON") & (f.col("FCN_ID_EDOCTA").isin(clients)))
                .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_CONCEPTO_NEGOCIO",
                        f.col("FTN_VALOR_ACTUAL_UDI").cast("decimal(16, 2)"),
                        f.col("FTN_VALOR_NOMINAL_UDI").cast("decimal(16, 2)"),
                        f.col("FTN_VALOR_ACTUAL_PESO").cast("decimal(16, 2)"),
                        f.col("FTN_VALOR_NOMINAL_PESO").cast("decimal(16, 2)"))

            )

            df_anverso_sdo = (
                df_anverso_general.filter((f.col("FTC_SECCION") == "SDO") & (f.col("FCN_ID_EDOCTA").isin(clients)))
                .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_GRUPO_CONCEPTO", "FTC_CONCEPTO_NEGOCIO",
                        f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))

            )

            general_count = df_general.count()
            ahorro_count = df_anverso_aho.count()
            bono_count = df_anverso_bon.count()
            saldo_count = df_anverso_sdo.count()

            processed_data = []
            processed_data += process_dataframe(df_general.fillna("").fillna(0), 1)
            processed_data += process_dataframe(df_anverso_aho.fillna("").fillna(0), 2)
            processed_data += process_dataframe(df_anverso_bon.fillna("").fillna(0), 3)
            processed_data += process_dataframe(df_anverso_sdo.fillna("").fillna(0), 4)

            res = "\n".join("|".join(str(item) for item in row) for row in processed_data)

            total_count = sum([general_count, ahorro_count, bono_count, saldo_count])

            final_row = f"\n5|{general_count}|{ahorro_count}|{bono_count}|{saldo_count}|{total_count}"
            data_strings = res + final_row

            name_anverso = f"recaudacion_anverso_mensual_{str(month)[:4]}_{str(month)[-2:]}_1-1.txt" if type(
                month) == int else f"recaudacion_anverso_cuatrimestral_{str(cuatrimestre)[:4]}_{str(cuatrimestre)[-2:]}_1-1.txt"

            str_to_gcs(data_strings, name_anverso)

            #upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_anverso, sftp_remote_file_path, data_strings)

            body_message += f"Se generó el archivo de {name_anverso} con un total de {total_count} registros\n"

            df_edo_reverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_REVERSO')

            df_reverso_general = df_edo_reverso.join(df_general, "FCN_NUMERO_CUENTA")
            df_reverso_general = (
                df_reverso_general.select("FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION",
                                          f.date_format("FTD_FECHA_MOVIMIENTO", "ddMMyyyy").alias(
                                              "FTD_FECHA_MOVIMIENTO"),
                                          "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO",
                                          f.col("FTN_SALARIO_BASE").cast("decimal(16, 2)"),
                                          f.col("FTN_MONTO").cast("decimal(16, 2)"))
            )

            reverso_data = process_dataframe(df_reverso_general.fillna("").fillna(0), 1)

            res = "\n".join("|".join(str(item) for item in row) for row in reverso_data)

            ret = df_reverso_general.filter(f.col("FTC_SECCION") == "RET").count()
            vol = df_reverso_general.filter(f.col("FTC_SECCION") == "VOL").count()
            viv = df_reverso_general.filter(f.col("FTC_SECCION") == "VIV").count()
            total = df_reverso_general.count()
            reverso_final_row = f"\n2|{ret}|{vol}|{viv}|{total}"
            final_reverso = res + reverso_final_row

            name_reverso = f"recaudacion_reverso_mensual_{str(month)[:4]}_{str(month)[-2:]}_1-1.txt" if type(
                month) == int else f"recaudacion_reverso_cuatrimestral_{str(cuatrimestre)[:4]}_{str(cuatrimestre)[-2:]}_1-1.txt"

            str_to_gcs(final_reverso, name_reverso)

            #upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_reverso, "", final_reverso)

            body_message += f"Se generó el archivo de {name_reverso} con un total de {total} registros\n"

        """send_email(
            host=smtp_host,
            port=smtp_port,
            username=smtp_user,
            password=smtp_pass,
            from_address=smtp_user,
            to_address="alfredo.guerra@profuturo.com.mx",
            subject="Generacion de los archivos de recaudacion",
            body=body_message
        )

        notify(
            postgres,
            f"Generacion de archivos Recaudaciones",
            phase,
            area,
            term=term_id,
            message=f"Se han exportado recaudaciones para el cuatrimestre {cuatrimestre}",
            aprobar=False,
            descarga=False
        )"""
