from profuturo.common import define_extraction, register_time
from profuturo.database import get_postgres_pool, configure_postgres_spark, get_bigquery_pool
from profuturo.extraction import read_table_insert_temp_view, extract_terms, _get_spark_session, _create_spark_dataframe
from pyspark.sql.functions import concat, col, row_number, lit, lpad, udf,date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DecimalType
from google.cloud import storage, bigquery
from profuturo.env import load_env
import requests
import sys
import math
import os



load_env()
postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
storage_client = storage.Client()
phase = int(sys.argv[1])
term = int(sys.argv[2])
user = int(sys.argv[3])
area = int(sys.argv[4])
print(phase,term, user,area)
bucket_name = os.getenv("BUCKET_ID")
print(bucket_name)
prefix =f"{os.getenv('PREFIX_BLOB')}"
print(prefix)
url = os.getenv("URL_MUESTRAS_RECA")
print(url)

decimal_pesos = DecimalType(16, 2)
decimal_udi = DecimalType(16, 2)

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
           10001033,10001034,10001035,10001036,10001037,10001038,10001039,10001040
           )
cuentas = (1330029515,1350011161,1530002222,3070006370,
           3200089837,3200474366,3200534369,3200767640,
           3200840759,3201096947,3201292580,3201900769)
def get_buckets():
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name.startswith("edo_cuenta_profuturo"):
            return bucket.name

bucket = storage_client.get_bucket(get_buckets())

def str_to_gcs(data, name):
    blob = bucket.blob(f"correspondencia/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")
    print(F"SE CREO EL ARCHIVO DE correspondencia/{name}")

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


def process_dataframe(df, identifier):
    return df.rdd.flatMap(lambda row: [([identifier] + [row[i] for i in range(len(row))])]).collect()


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 100)
    spark.conf.set("spark.default.parallelism", 100)

    with register_time(postgres_pool, phase, term_id, user, area):
        body_message = ""
        query = f"""
        SELECT
        DISTINCT
        1 AS "NUMERO_SECCION",
        "FCN_NUMERO_CUENTA",
        "FCN_FOLIO",
        "FTC_NOMBRE_COMPLETO",
        "FTC_CALLE_NUMERO",
        "FTC_COLONIA",
        "FTC_DELEGACION",
        "FTN_CP",
        "FTC_ENTIDAD_FEDERATIVA",
        "FTC_RFC",
        "FTC_NSS",
        "FTC_CURP",
        to_char("FTD_FECHA_GRAL_INICIO", 'ddmmyyyy') AS "FTD_FECHA_GRAL_INICIO",
        to_char("FTD_FECHA_GRAL_FIN",'ddmmyyyy') AS "FTD_FECHA_GRAL_FIN",
        CASE
        WHEN "FTC_TIPOGENERACION" = 'IMSS - ISSSTE (MIXTO)' THEN 'MIX'
        WHEN "FTC_TIPOGENERACION" = 'GENERACIÓN AFORE' THEN 'GAF'
        WHEN "FTC_TIPOGENERACION" = 'GENERACIÓN DE TRANSICIÓN' THEN 'MIX'
        ELSE 'DEC' END "FTC_GENERACION",
        "FTC_DESC_SIEFORE",
        to_char("FTD_FECHA_CORTE", 'ddmmyyyy') AS "FTD_FECHA_CORTE",
        'SIN BLOQUEO' AS "BLOQUEO_IMPRESION",
        "FTN_SALDO_SUBTOTAL",
        "FTN_SALDO_TOTAL",
        "FTN_PENSION_MENSUAL",
        "FTC_TIPO_TRABAJADOR",
        "FTC_FORMATO" --TO_DO CAMBIAR POR PERIODICIDAD DE FOMRATO
        FROM "ESTADO_CUENTA"."TTEDOCTA_GENERAL_TEST" G
        LEFT JOIN "ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR" I
        ON "FCN_NUMERO_CUENTA" = "FCN_CUENTA"
        WHERE -- "FTB_IMPRESION" = TRUE
        --AND "FTB_GENERACION" = TRUE
        --AND "FTB_ENVIO" = FALSE
        --AND I."FCN_ID_PERIODO" = :term
        --AND 
        "FCN_NUMERO_CUENTA" IN {cuentas}
        """
        general_df = _create_spark_dataframe(spark, configure_postgres_spark, query,
        params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        general_df = general_df.withColumn("FCN_NUMERO_CUENTA", lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))

        general_df = general_df.repartition("FTC_GENERACION","FTC_ENTIDAD_FEDERATIVA")


        query_anverso = f"""
        SELECT 
        DISTINCT
        CASE
        WHEN "FTC_SECCION" = 'SDO' THEN 4
        WHEN "FTC_SECCION" IN( 'AHO', 'PEN') THEN 2
        WHEN "FTC_SECCION" = 'BON' THEN 3
        END  "NUMERO_SECCION",
        "FCN_NUMERO_CUENTA",
        "FTC_CONCEPTO_NEGOCIO",
        "FTN_SALDO_ANTERIOR",
        "FTF_APORTACION",
        "FTN_RETIRO",
        "FTN_RENDIMIENTO",
        "FTN_COMISION",
        "FTN_SALDO_FINAL",
        "FTN_VALOR_ACTUAL_UDI",
        "FTN_VALOR_NOMINAL_UDI",
        "FTN_VALOR_ACTUAL_PESO",
        "FTN_VALOR_NOMINAL_PESO",
        CASE
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%RET%' THEN 'AHORET'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%FOVIS%' THEN 'AHOFOV'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%VIV%' THEN 'AHOVIV'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%VOL%' THEN 'AHOVOL'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%SAR%' THEN 'SAR92'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%VEJEZ%' THEN 'IMSSCE'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%ISSSTE 2008%' THEN 'ISSSTE08'
        END "GRUPO_CONCEPTO"
        FROM "ESTADO_CUENTA"."TTEDOCTA_ANVERSO_TEST" A
        LEFT JOIN "ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR" I
        ON "FCN_NUMERO_CUENTA" = "FCN_CUENTA"
        WHERE --"FTB_IMPRESION" = TRUE
        --AND "FTB_GENERACION" = TRUE
        --AND "FTB_ENVIO" = FALSE
        --AND I."FCN_ID_PERIODO" = :term
        --AND 
        "FCN_NUMERO_CUENTA" IN {cuentas}
        """
        anverso_df =_create_spark_dataframe(spark, configure_postgres_spark, query_anverso,params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        anverso_seccion_ahor = anverso_df.select(col("NUMERO_SECCION").alias("NUMERO_SECCION_AHO"),lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0").alias("FCN_NUMERO_CUENTA_AHO"),
                                                 col("FTC_CONCEPTO_NEGOCIO").alias("FTC_CONCEPTO_NEGOCIO_AHO"),
                                                 col("FTN_SALDO_ANTERIOR"),col("FTF_APORTACION"),col("FTN_RETIRO"),
                                                 col("FTN_RENDIMIENTO"),
                                                 col("FTN_COMISION"),
                                                 col("FTN_SALDO_FINAL")
                                                 ).filter(col("NUMERO_SECCION") == 2)

        anverso_seccion_ahor = anverso_seccion_ahor.repartition("FTC_CONCEPTO_NEGOCIO")

        anverso_seccion_bono = anverso_df.select(col("NUMERO_SECCION").alias("NUMERO_SECCION_BON"),lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0").alias("FCN_NUMERO_CUENTA_BON"),
                                                 col("FTC_CONCEPTO_NEGOCIO").alias("FTC_CONCEPTO_NEGOCIO_BON"), col("FTN_VALOR_ACTUAL_UDI"),
                                                col("FTN_VALOR_NOMINAL_UDI"), col("FTN_VALOR_ACTUAL_PESO"),
                                                col("FTN_VALOR_NOMINAL_PESO")).filter(col("NUMERO_SECCION") == 3)


        anverso_seccion_sdo = anverso_df.select(col("NUMERO_SECCION").alias("NUMERO_SECCION_SDO"),lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0").alias("FCN_NUMERO_CUENTA_SDO"),
                                                col("FTC_CONCEPTO_NEGOCIO").alias("FTC_CONCEPTO_NEGOCIO_SDO"),col("GRUPO_CONCEPTO"),
                                                col("FTN_SALDO_FINAL").alias("FTN_SALDO_FINAL_SDO")
                                                ).filter(col("NUMERO_SECCION") == 4)

        anverso_seccion_sdo = anverso_seccion_sdo.repartition("FTC_CONCEPTO_NEGOCIO","GRUPO_CONCEPTO")

        joined_anverso = (
            general_df
            .join(anverso_seccion_ahor,general_df.FCN_NUMERO_CUENTA == anverso_seccion_ahor.FCN_NUMERO_CUENTA_AHO, 'LEFT')
            .join(anverso_seccion_bono, general_df.FCN_NUMERO_CUENTA == anverso_seccion_bono.FCN_NUMERO_CUENTA_BON,
                  'LEFT')
            .join(anverso_seccion_sdo, general_df.FCN_NUMERO_CUENTA == anverso_seccion_sdo.FCN_NUMERO_CUENTA_SDO,
                  'LEFT')
        )

        joined_anverso = joined_anverso.withColumn("id", monotonically_increasing_id())

        # Obtener el número total de registros
        total_records = joined_anverso.count()

        # Definir el tamaño de la partición
        partition_size = 500_000

        # Inicializar la variable de índice de inicio
        start_index = 0
        part = 1

        while start_index < total_records:
            # Seleccionar los registros de la partición actual
            partition = joined_anverso.limit(partition_size).filter(joined_anverso.id >= start_index)

            general_df = joined_anverso.select(
                col("FCN_NUMERO_CUENTA").alias("FCN_NUMERO_CUENTA"),
                col("FCN_FOLIO").alias("FCN_FOLIO"),
                col("FTC_NOMBRE_COMPLETO").alias("FTC_NOMBRE_COMPLETO"),
                col("FTC_CALLE_NUMERO").alias("FTC_CALLE_NUMERO"),
                col("FTC_COLONIA").alias("FTC_COLONIA"),
                col("FTC_DELEGACION").alias("FTC_DELEGACION"),
                col("FTN_CP").alias("FTN_CP"),
                col("FTC_ENTIDAD_FEDERATIVA").alias("FTC_ENTIDAD_FEDERATIVA"),
                col("FTC_RFC").alias("FTC_RFC"),
                col("FTC_NSS").alias("FTC_NSS"),
                col("FTC_CURP").alias("FTC_CURP"),
                col("FTD_FECHA_GRAL_INICIO").alias("FTD_FECHA_GRAL_INICIO"),
                col("FTD_FECHA_GRAL_FIN").alias("FTD_FECHA_GRAL_FIN"),
                col("FTC_GENERACION").alias("FTC_GENERACION"),
                col("FTC_DESC_SIEFORE").alias("FTC_DESC_SIEFORE"),
                col("FTD_FECHA_CORTE").alias("FTD_FECHA_CORTE"),
                col("BLOQUEO_IMPRESION").alias("BLOQUEO_IMPRESION"),
                col("FTN_SALDO_SUBTOTAL").cast(decimal_pesos).alias("FTN_SALDO_SUBTOTAL"),
                col("FTN_SALDO_TOTAL").cast(decimal_pesos).alias("FTN_SALDO_TOTAL"),
                col("FTN_PENSION_MENSUAL").cast(decimal_pesos).alias("FTN_PENSION_MENSUAL"),
                col("FTC_TIPO_TRABAJADOR").alias("FTC_TIPO_TRABAJADOR"),
                col("FTC_FORMATO").alias("FTC_FORMATO")
            )

            general_df = general_df.dropDuplicates()

            anverso_seccion_ahor = joined_anverso.select(
                                                     col("FCN_NUMERO_CUENTA_AHO"),
                                                     col("FTC_CONCEPTO_NEGOCIO_AHO"),
                                                     col("FTN_SALDO_ANTERIOR").cast(decimal_pesos),
                                                     col("FTF_APORTACION").cast(decimal_pesos),
                                                     col("FTN_RETIRO").cast(decimal_pesos),
                                                     col("FTN_RENDIMIENTO").cast(decimal_pesos),
                                                     col("FTN_COMISION").cast(decimal_pesos),
                                                     col("FTN_SALDO_FINAL").cast(decimal_pesos)
            )

            anverso_seccion_ahor = anverso_seccion_ahor.dropDuplicates()

            anverso_seccion_bono = joined_anverso.select(
                                                     col("FCN_NUMERO_CUENTA_BON"),
                                                     col("FTC_CONCEPTO_NEGOCIO_BON"), col("FTN_VALOR_ACTUAL_UDI").cast(decimal_udi),
                                                     col("FTN_VALOR_NOMINAL_UDI").cast(decimal_udi), col("FTN_VALOR_ACTUAL_PESO").cast(decimal_pesos),
                                                     col("FTN_VALOR_NOMINAL_PESO").cast(decimal_pesos))

            anverso_seccion_bono = anverso_seccion_bono.dropDuplicates()

            anverso_seccion_sdo = joined_anverso.select(
                                                    col("FCN_NUMERO_CUENTA_SDO"),
                                                    col("FTC_CONCEPTO_NEGOCIO_SDO"), col("GRUPO_CONCEPTO"),
                                                    col("FTN_SALDO_FINAL_SDO").cast(decimal_pesos)
                                                    )

            anverso_seccion_sdo = anverso_seccion_sdo.dropDuplicates()

            general_count = general_df.count()
            ahorro_count = anverso_seccion_ahor.count()
            bono_count = anverso_seccion_bono.count()
            saldo_count = anverso_seccion_sdo.count()
            total_count = sum([general_count, ahorro_count, bono_count, saldo_count])
            print("COUNTERS: ", general_count, ahorro_count, bono_count, saldo_count, total_count)

            processed_data = []
            processed_data += process_dataframe(general_df.fillna("").fillna(0), 1)
            processed_data += process_dataframe(anverso_seccion_ahor.fillna("").fillna(0), 2)
            processed_data += process_dataframe(anverso_seccion_bono.fillna("").fillna(0), 3)
            processed_data += process_dataframe(anverso_seccion_sdo.fillna("").fillna(0), 4)
            print("DATOS PROCESADOS ANVERSO")

            processed_data_part = processed_data
            res = "\n".join("|".join(str(item) for item in row) for row in processed_data_part)
            final_row = f"\n5|{general_count}|{ahorro_count}|{bono_count}|{saldo_count}|{total_count}"
            res = res + final_row

            name_anverso = f"recaudacion_anverso_cuatrimestral_{term_id}_{part}.txt"

            str_to_gcs(res, name_anverso)
            # upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_anverso, sftp_remote_file_path, res)
            body_message += f"Se generó el archivo de {name_anverso} con un total de {general_count} registros\n"

            # Actualizar el índice de inicio para la próxima partición
            start_index += partition_size

            part += 1

        query_reverso = f"""
                        SELECT * FROM "ESTADO_CUENTA"."TTEDOCTA_REVERSO_TEST"  WHERE "FCN_NUMERO_CUENTA" IN {cuentas}
                                        """
        read_table_insert_temp_view(
            configure_postgres_spark,
            query_reverso,
            "reverso"
        )

        df_edo_reverso = spark.sql("SELECT * FROM reverso")
        df_edo_reverso = df_edo_reverso.withColumn("FCN_NUMERO_CUENTA", lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))

        conteo_reverso = df_edo_reverso.count()
        print("SE EXTRAJO EXITOSAMENTE REVERSO", conteo_reverso)

        df_reverso_general = df_edo_reverso.join(general_df, "FCN_NUMERO_CUENTA")
        print("SE REALIZO EL JOIN DE REVERSO", df_reverso_general.count())
        df_reverso_general = (
            df_reverso_general.select("FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION",
                                      date_format("FTD_FECHA_MOVIMIENTO", "ddMMyyyy").alias(
                                          "FTD_FECHA_MOVIMIENTO"),
                                      "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO",
                                      col("FTN_SALARIO_BASE").cast("decimal(16, 2)"),
                                      col("FTN_MONTO").cast("decimal(16, 2)"))
        )
        print("DF_REVERSO_GENERAL", df_reverso_general.count())
        ret = df_reverso_general.filter(col("FTC_SECCION") == "RET").count()
        print("RET", ret)
        vol = df_reverso_general.filter(col("FTC_SECCION") == "VOL").count()
        print("VOL", vol)
        viv = df_reverso_general.filter(col("FTC_SECCION") == "VIV").count()
        print("VIV", viv)
        total = df_reverso_general.count()

        num_parts_reverso = math.ceil(total / partition_size)

        reverso_data = process_dataframe(df_reverso_general.fillna("").fillna(0), 1)
        print("DATOS PROCESADOS REVERSO")

        for part in range(1, num_parts_reverso + 1):
            if len(reverso_data) < partition_size:
                reverso_data_part = reverso_data
                res = "\n".join("|".join(str(item) for item in row) for row in reverso_data_part)
                reverso_final_row = f"\n2|{ret}|{vol}|{viv}|{total}"
                res = res + reverso_final_row
            else:
                reverso_data_part = reverso_data[:partition_size]
                res = "\n".join("|".join(str(item) for item in row) for row in reverso_data_part)
                del reverso_data[:partition_size - 1]

            name_reverso = f"recaudacion_reverso_cuatrimestral_{term_id}_{part}-{num_parts_reverso}.txt"
            str_to_gcs(res, name_reverso)
            # upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_reverso, "", res)
            body_message += f"Se generó el archivo de {name_reverso} con un total de {conteo_reverso} registros\n"

        send_email(
            to_address="zaletadev@gmail.com",
            subject="Generacion de los archivos de recaudacion",
            body=body_message
        )





