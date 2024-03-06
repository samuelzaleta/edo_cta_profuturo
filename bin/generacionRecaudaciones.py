import sys
import math
from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, configure_postgres_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view
from pyspark.sql.functions import lit
from profuturo.reporters import HtmlReporter
from google.cloud import storage, secretmanager
import pyspark.sql.functions as f
import os
import requests
from profuturo.env import load_env




load_env()


spark = _get_spark_session()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
#url = 'http://10.60.23.19:5006/procesos/email/send'
#url = 'https://procesos-api-service-h3uy3grcoq-uk.a.run.app/procesos/email/send'
url ='https://10.60.1.8:443/procesos/email/send'

phase = int(sys.argv[1])
cuatrimestre = int(sys.argv[2][-2:])
user = int(sys.argv[3])
area = int(sys.argv[4])

storage_client = storage.Client()

host = os.getenv("SMTP_HOST")
addres = os.getenv("SMTP_ADDRESS_SENDER")
password= os.getenv("SMTP_PASSWORD")
port = os.getenv("SMTP_PORT")

smtp_host = host
smtp_port = port
smtp_user = addres
smtp_pass = password


cuentas = (10000851,10000861,10000868,10000872,1330029515,1350011161,1530002222,1700004823,3070006370,3200089837,
           3200231348,3200534369,3201895226,3201900769,3202077144,3202135111,3300118473,3300576485,3300797221,3300809724,
           3400764001,3500053269,3500058618,6120000991,6442107959,6442128265,6449009395,6449015130,10000884,10000885,
           10000887,10000888,10000889,10000890,10000891,10000892,10000893,10000894,10000895,10000896,10001041,10001042,
           10000898,10000899,10000900,10000901,10000902,10000903,10000904,10000905,10000906,10000907,10000908,10000909,
           10000910,10000911,10000912,10000913,10000914,10000915,10000916,10000917,10000918,10000919,10000920,10000921,
           10000922,10000923,10000924,10000925,10000927,10000928,10000929,10000930,10000931,10000932,10000933,10000934,
           10000935,10000936,10001263,10001264,10001265,10001266,10001267,10001268,10001269,10001270,10001271,10001272,
           10001274,10001275,10001277,10001278,10001279,10001280,10001281,10001282,10001283,10001284,10001285,10001286,
           10001288,10001289,10001290,10001292,10001293,10001294,10001296,10001297,10001298,10001299,10001300,10001301,
           10001305,10001306,10001307,10001308,10001309,10001311,10001312,10001314,10001315,10001316,10001317,10001318,
           10001319,10001320,10001321,10001322,10000896,10000898,10000790,10000791,10000792,10000793,10000794,10000795,
           10000797,10000798,10000799,10000800,10000801,10000802,10000803,10000804,10000805,10000806,10000807,10000808,
           10000809,10000810,10000811,10000812,10000813,10000814,10000815,10000816,10000817,10000818,10000819,10000820,
           10000821,10000822,10000823,10000824,10000825,10000826,10000827,10000828,10000830,10000832,10000833,10000834,
           10000835,10000836,10000837,10000838,10000839,10000840,10001098,10001099,10001100,10001101,10001102,10001103,
           10001104,10001105,10001106,10001107,10001108,10001109,10001110,10001111,10001112,10001113,10001114,10001115,
           10001116,10001117,10001118,10001119,10001120,10001121,10001122,10001123,10001124,10001125,10001126,10001127,
           10001128,10001129,10001130,10001131,10001132,10001133,10001134,10001135,10001136,10001137,10001138,10001139,
           10001140,10001141,10001142,10001143,10001145,10001146,10001147,10001148,10000991,10000992,10000993,10000994,
           10000995,10000996,10000997,10000998,10000999,10001000,10001001,10001002,10001003,10001004,10001005,10001006,
           10001007,10001008,10001009,10001010,10001011,10001012,10001013,10001014,10001015,10001016,10001017,10001018,
           10001019,10001020,10001021,10001023,10001024,10001025,10001026,10001027,10001029,10001030,10001031,10001032,
           10001033,10001034,10001035,10001036,10001037,10001038,10001039,10001040)
def get_buckets():
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name.startswith("edo_cuenta_profuturo"):
            return bucket.name


bucket = storage_client.get_bucket(get_buckets())


def process_dataframe(df, identifier):
    return df.rdd.flatMap(lambda row: [([identifier] + [row[i] for i in range(len(row))])]).collect()


def str_to_gcs(data, name):
    blob = bucket.blob(f"correspondencia/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")
    print(F"SE CREO EL ARCHIVO DE correspondencia/{name}")


def get_months(cuatrimestre):
    year = str(cuatrimestre)[:4]
    print("year", year)
    if int(sys.argv[2][-2:]) in (1, 2, 3, 4):
        cuatrimestre = 1
    elif int(sys.argv[2][-2:]) in (5, 6, 7, 8):
        cuatrimestre = 2
    else:
        cuatrimestre = 3
    last_month = (cuatrimestre * 3) + cuatrimestre
    first_month = last_month - 3
    months = [int(year + "0" + str(month)) for month in range(first_month, last_month + 1)]
    print("months", months)
    return months




def send_email( to_address, subject, body):

    message = "{}\n\n{}".format(subject, body)
    datos = {
        'to': to_address,
        'subject': subject,
        'text': message
    }
    response = requests.post(url, data=datos,  verify=False)

    if response.status_code == 200:
        # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
        print("La solicitud fue exitosa")
    else:
        # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
        print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")


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


        print(f"GENERACION DE RECAUDACIONES PARA EL MES {term_id}")

        query_indicador = """
                    SELECT DISTINCT "FCN_CUENTA" FROM "ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR"
                    WHERE "FTB_IMPRESION" = TRUE
                    AND "FTB_ENVIO" = FALSE AND "FCN_ID_PERIODO" = :term
                    """
        read_table_insert_temp_view(
            configure_postgres_spark,
            query_indicador,
            "indicador", params={'term': term_id}
        )

        query_general = f"""
                                                SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_GENERAL" WHERE "FCN_NUMERO_CUENTA" IN {cuentas}
                                                """
        read_table_insert_temp_view(
            configure_postgres_spark,
            query_general,
            "general"
        )

        df_general = spark.sql("SELECT * FROM general")
        df_indicador = spark.sql("SELECT * FROM indicador")

        if type(term_id) == list:
            df_general = (
                df_general.filter(f.col("FCN_ID_PERIODO").isin(term_id))
            )
            print("count filtro 1",df_general.count())

        df_general = (
            df_general.withColumn("FCN_NUMERO_CUENTA", f.lpad(f.col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))
        )

        df_indicador = (
            df_indicador.withColumn("FCN_CUENTA", f.lpad(f.col("FCN_CUENTA").cast("string"), 10, "0"))
        )
        print("count filtro 3", df_general.count())

        df_general = df_general.join(df_indicador, df_general.FCN_NUMERO_CUENTA == df_indicador.FCN_CUENTA,
                                     'left')
        conteo_general =df_general.count()
        print("count",conteo_general)
        if df_general.count() > 0:

            df_general.cache()

            query_anverso = f"""
                                                SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_ANVERSO"  WHERE "FCN_NUMERO_CUENTA" IN {cuentas}
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
            general_count = df_general.count()
            print("DF_GENERAL",general_count)

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
                .select("FCN_NUMERO_CUENTA_ANVERSO", lit('').alias("FTC_GRUPO_CONCEPTO"), "FTC_CONCEPTO_NEGOCIO",
                        f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))

            )

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

                name_anverso = f"recaudacion_anverso_cuatrimestral_{term_id}_{part}-{num_parts_anverso}.txt"

                str_to_gcs(res, name_anverso)
                # upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_anverso, sftp_remote_file_path, res)
                body_message += f"Se generó el archivo de {name_anverso} con un total de {conteo_general} registros\n"

            query_reverso = f"""
                SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_REVERSO"  WHERE "FCN_NUMERO_CUENTA" IN {cuentas}
                                """
            read_table_insert_temp_view(
                configure_postgres_spark,
                query_reverso,
                "reverso"
            )

            df_edo_reverso = spark.sql("SELECT * FROM reverso")
            conteo_reverso = df_edo_reverso.count()
            print("SE EXTRAJO EXITOSAMENTE REVERSO", conteo_reverso )

            df_reverso_general = df_edo_reverso.join(df_general, "FCN_NUMERO_CUENTA")
            print("SE REALIZO EL JOIN DE REVERSO", df_reverso_general.count())
            df_reverso_general = (
                df_reverso_general.select("FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION",
                                          f.date_format("FTD_FECHA_MOVIMIENTO", "ddMMyyyy").alias(
                                              "FTD_FECHA_MOVIMIENTO"),
                                          "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO",
                                          f.col("FTN_SALARIO_BASE").cast("decimal(16, 2)"),
                                          f.col("FTN_MONTO").cast("decimal(16, 2)"))
            )
            print("DF_REVERSO_GENERAL", df_reverso_general.count())
            ret = df_reverso_general.filter(f.col("FTC_SECCION") == "RET").count()
            print("RET", ret)
            vol = df_reverso_general.filter(f.col("FTC_SECCION") == "VOL").count()
            print("VOL", vol)
            viv = df_reverso_general.filter(f.col("FTC_SECCION") == "VIV").count()
            print("VIV", viv)
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

                name_reverso = f"recaudacion_reverso_cuatrimestral_{term_id}_{part}-{num_parts_reverso}.txt"
                str_to_gcs(res, name_reverso)
                # upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_reverso, "", res)
                body_message += f"Se generó el archivo de {name_reverso} con un total de {conteo_reverso} registros\n"

    send_email(
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
    )
