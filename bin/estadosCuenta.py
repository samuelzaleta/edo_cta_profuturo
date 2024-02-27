from profuturo.common import define_extraction, register_time, notify, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, _get_spark_session, _create_spark_dataframe
from pyspark.sql.functions import concat, col, row_number, lit, lpad
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage, bigquery
from pyspark.sql.window import Window
from profuturo.env import load_env
from sqlalchemy import text
from io import BytesIO
from PIL import Image
import pyspark.sql.functions as F
import requests
import random
import string
import json
import sys
import os

load_env()
postgres_pool = get_postgres_pool()
storage_client = storage.Client()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
bucket_name = os.getenv("BUCKET_DEFINITIVO")
bucket_coldline = os.getenv("BUCKET_COLDLINE")
bucket_archive = os.getenv("BUCKET_NEARLINE")
print(bucket_name)
prefix_definitivos =f"{os.getenv('PREFIX_DEFINITIVO')}"
prefix =f"{os.getenv('PREFIX_BLOB')}"
print(prefix_definitivos)
url = os.getenv("URL_DEFINITIVO_RECA")
print(url)
prefix_term_id = (prefix + '_'+ sys.argv[2])
print(prefix_term_id)

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

cuentas = (3202077144,3500053269) #,3500053269,6442107959, 10000915, 3070006370,3202077144)

def upload_to_gcs(row):
    id_value = row["id"]
    bytea_data = row["fto_imagen"]

    # Convertir bytes a imagen
    image = Image.open(BytesIO(bytea_data))

    # Guardar imagen localmente (opcional)
    # image.save(f"local/{id_value}.png")

    # Subir imagen a GCS
    blob_name = f"{prefix_term_id}/{id_value}.png"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Convertir imagen a bytes antes de subirla
    byte_stream = BytesIO()
    image.save(byte_stream, format="PNG")
    byte_stream.seek(0)

    blob.upload_from_file(byte_stream, content_type="image/png")

def delete_all_objects(bucket_name, prefix):
    # Crea una instancia del cliente de Cloud Storage
    storage_client = storage.Client()

    # Obtiene el bucket
    bucket = storage_client.bucket(bucket_name)

    # Lista todos los objetos en el bucket con el prefijo especificado
    blobs = bucket.list_blobs(prefix=prefix)

    # Elimina cada objeto
    for blob in blobs:
        #print(f"Eliminando objeto: {blob.name}")
        blob.delete()

def get_blob_info(bucket_name, prefix):
    # Crea una instancia del cliente de Cloud Storage
    storage_client = storage.Client()

    # Obtiene el bucket
    bucket = storage_client.bucket(bucket_name)

    # Lista todos los objetos en el bucket con el prefijo especificado
    blobs = bucket.list_blobs(prefix=prefix)

    # Lista para almacenar información de blobs
    blob_info_list = []

    # Recorre todos los blobs y obtiene información
    for blob in blobs:
        # Divide el nombre del blob en partes usando '-'
        parts = blob.name.split('-')

        # Asegúrate de que haya al menos tres partes en el nombre
        if len(parts) == 4:
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2].split('.')[0]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_SIEFORE": parts[3].split('.')[0] if parts[3].split('.')[0] != 'sinsiefore' else None
            }

            blob_info_list.append(blob_info)

        if len(parts) > 4:
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2].split('.')[0]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_SIEFORE": f"{parts[3]}-{parts[4].split('.')[0]}"
            }

            blob_info_list.append(blob_info)

    return blob_info_list

def move_blob(source_bucket, destination_bucket, source_blob_name, destination_blob_name):
    source_blob = source_bucket.blob(source_blob_name)
    destination_blob = destination_bucket.blob(destination_blob_name)

    # Copiar el blob del bucket fuente al bucket de destino
    destination_blob.rewrite(source_blob)

def move_files_parallel(source_bucket_name, destination_bucket_name, source_prefix="", destination_prefix="", num_threads=10):
    # Inicializa los clientes de almacenamiento
    source_client = storage.Client()
    destination_client = storage.Client()
    print("\n",source_bucket_name)
    print(destination_bucket_name)
    # Obtén los buckets
    source_bucket = source_client.get_bucket(source_bucket_name)
    destination_bucket = destination_client.get_bucket(destination_bucket_name)

    # Lista todos los archivos en el bucket fuente con el prefijo dado
    blobs = source_bucket.list_blobs(prefix=source_prefix)

    # Usa ThreadPoolExecutor para ejecutar la copia de blobs en paralelo
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []

        for blob in blobs:
            # Crear el nombre del blob de destino con el prefijo de destino
            destination_blob_name = destination_prefix + blob.name[len(source_prefix):]
            futures.append(executor.submit(move_blob, source_bucket, destination_bucket, blob.name, destination_blob_name))

        # Espera a que todos los hilos hayan completado
        for future in futures:
            future.result()

    print("Movimiento de archivos completado")


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session(
    excuetor_memory = '16g',
    memory_overhead ='1g',
    memory_offhead ='1g',
    driver_memory ='2g',
    intances = 4,
    parallelims = 18000)
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")
    spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 20)


    with register_time(postgres_pool, phase, term_id, user, area):

        ###########################  IMAGENES   #################################################
        truncate_table(postgres, 'TTEDOCTA_IMAGEN')

        delete_all_objects(bucket_name, prefix_term_id)

        move_files_parallel(bucket_name, bucket_name, source_prefix="profuturo-archivos",destination_prefix=prefix_term_id)

        query = """
                       SELECT
                       DISTINCT
                       concat("FTC_CODIGO_POSICION_PDF",'-',tcie."FCN_ID_FORMATO_ESTADO_CUENTA",'-', ra."FCN_ID_AREA",'-',COALESCE(tcie."FTC_DESCRIPCION_SIEFORE",'sinsiefore')) AS ID,"FTO_IMAGEN" AS FTO_IMAGEN
                       FROM "GESTOR"."TTGESPRO_CONFIG_IMAGEN_EDOCTA" tcie
                       INNER JOIN "GESTOR"."TTGESPRO_ROL_USUARIO" ru ON CAST(tcie."FTC_USUARIO" AS INT) = ru."FCN_ID_USUARIO"
                       INNER JOIN "GESTOR"."TCGESPRO_ROL_AREA" ra ON ru."FCN_ID_ROL" =  ra."FCN_ID_ROL"
                       """

        imagenes_df = _create_spark_dataframe(spark, configure_postgres_spark, query,
                                              params={"term": term_id, "start": start_month, "end": end_month,
                                                      "user": str(user)})

        imagenes_df.show()

        imagenes_df.foreach(upload_to_gcs)

        # Obtiene la información del blob
        blob_info_list = get_blob_info(bucket_name, prefix)

        schema = StructType([
            StructField("FTC_POSICION_PDF", StringType(), True),
            StructField("FCN_ID_FORMATO_EDOCTA", IntegerType(), True),
            StructField("FCN_ID_AREA", IntegerType(), True),
            StructField("FTC_URL_IMAGEN", StringType(), True),
            StructField("FTC_IMAGEN", StringType(), True),
            StructField("FTC_SIEFORE", StringType(), True)
        ])

        df = spark.createDataFrame(blob_info_list, schema=schema)

        _write_spark_dataframe(df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_IMAGEN"')

        ########################## GENERACIÓN DE MUESTRAS #################################################

        print("truncate general")
        postgres.execute(text("""TRUNCATE TABLE "ESTADO_CUENTA"."TTEDOCTA_GENERAL_TEST"  """), {'end': end_month})
        print("truncate reverso")
        postgres.execute(text("""TRUNCATE TABLE "ESTADO_CUENTA"."TTEDOCTA_REVERSO_TEST" """), {'end': end_month})
        print("truncate anverso")
        postgres.execute(text("""TRUNCATE TABLE "ESTADO_CUENTA"."TTEDOCTA_ANVERSO_TEST" """), {'end': end_month})

        char1 = random.choice(string.ascii_letters).upper()
        char2 = random.choice(string.ascii_letters).upper()
        random = char1 + char2
        print(random)

        movimientos_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH periodos AS (
        SELECT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_REVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
        )
        SELECT DISTINCT 
        "FCN_CUENTA", "FCN_ID_PERIODO", "FTF_MONTO_PESOS","FTN_SUA_DIAS_COTZDOS_BIMESTRE",
        "FTN_SUA_ULTIMO_SALARIO_INT_PER","FTD_FEH_LIQUIDACION", "FCN_ID_CONCEPTO_MOVIMIENTO",
        "FTC_SUA_RFC_PATRON","TIPO_SUBCUENA", "FTN_MONPES"
        FROM (
        SELECT
        R."FCN_CUENTA", R."FCN_ID_PERIODO", R."FTF_MONTO_PESOS",
        R."FTN_SUA_DIAS_COTZDOS_BIMESTRE",R."FTN_SUA_ULTIMO_SALARIO_INT_PER",
        R."FTD_FEH_LIQUIDACION", R."FCN_ID_CONCEPTO_MOVIMIENTO",R."FTC_SUA_RFC_PATRON",
        SB."FTC_TIPO_CLIENTE" AS "TIPO_SUBCUENA", 0 AS "FTN_MONPES"
        FROM "HECHOS"."TTHECHOS_MOVIMIENTO" R
        INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" SB
        ON SB."FTN_ID_TIPO_SUBCTA" = R."FCN_ID_TIPO_SUBCTA"
        INNER JOIN periodos ON R."FCN_ID_PERIODO" BETWEEN periodos.PERIODO_INICIAL AND periodos.PERIODO_FINAL
        WHERE R."FCN_CUENTA" IN {cuentas}
        
        UNION ALL
        
        SELECT
        R."CSIE1_NUMCUE",R."FCN_ID_PERIODO",R."MONTO" AS "FTF_MONTO_PESOS",
        NULL AS "FTN_SUA_DIAS_COTZDOS_BIMESTRE",NULL AS "FTN_SUA_ULTIMO_SALARIO_INT_PER",
        to_date(cast(R."CSIE1_FECCON" as varchar),'YYYYMMDD') AS "FTD_FEH_LIQUIDACION",
        CAST(R."CSIE1_CODMOV"AS INT) AS "FCN_ID_CONCEPTO_MOVIMIENTO",
        NULL AS "FTC_SUA_RFC_PATRON",SB."FTC_TIPO_CLIENTE" AS "TIPO_SUBCUENA",
        MP."FTN_MONPES"
        FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY" R
        INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" MP
        ON R."SUBCUENTA" = MP."FCN_ID_TIPO_SUBCUENTA" AND CAST(R."CSIE1_CODMOV" AS INT) = MP."FTN_ID_MOVIMIENTO_PROFUTURO"
        INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" SB
        ON SB."FTN_ID_TIPO_SUBCTA" = R."SUBCUENTA"
        INNER JOIN periodos ON R."FCN_ID_PERIODO" BETWEEN periodos.PERIODO_INICIAL AND periodos.PERIODO_FINAL
        WHERE R."CSIE1_NUMCUE" IN {cuentas}
        ) X
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        spark.sql("DROP TABLE IF EXISTS TTHECHOS_MOVIMIENTO")

        movimientos_df.write.partitionBy("FCN_ID_PERIODO", "FCN_ID_CONCEPTO_MOVIMIENTO") \
            .option("path", f"gs://{bucket_name}/datawarehouse/movimientos/TTHECHOS_MOVIMIENTO") \
            .option("mode", "append") \
            .option("compression", "snappy") \
            .saveAsTable("TTHECHOS_MOVIMIENTO")

        spark.sql(""" select  * from TTHECHOS_MOVIMIENTO """).show(60)

        rendimiento_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH periodos AS (
        SELECT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
        )
        SELECT
        DISTINCT
        "FCN_CUENTA", "FCN_ID_PERIODO", "FTF_SALDO_FINAL", "FTF_ABONO", "FTF_SALDO_INICIAL",
        "FTF_COMISION", "FTF_CARGO", "FTF_RENDIMIENTO_CALCULADO", "FTD_FECHAHORA_ALTA", "FCN_ID_TIPO_SUBCTA"
        FROM "HECHOS"."TTCALCUL_RENDIMIENTO" R
        INNER JOIN periodos ON R."FCN_ID_PERIODO" BETWEEN periodos.PERIODO_INICIAL AND periodos.PERIODO_FINAL
        WHERE R."FCN_CUENTA" IN {cuentas}
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        spark.sql("DROP TABLE IF EXISTS TTCALCUL_RENDIMIENTO")

        rendimiento_df.write.partitionBy("FCN_ID_PERIODO", "FCN_ID_TIPO_SUBCTA") \
            .option("path", f"gs://{bucket_name}/datawarehouse/rendimientos/TTCALCUL_RENDIMIENTO") \
            .option("mode", "append") \
            .option("compression", "snappy") \
            .saveAsTable("TTCALCUL_RENDIMIENTO")

        spark.sql(""" select  * from TTCALCUL_RENDIMIENTO WHERE FCN_ID_PERIODO = 202304 order by FCN_CUENTA """).show(80)

        dataset_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT
        DISTINCT
        I."FCN_CUENTA", I."FCN_ID_PERIODO", I."FTB_PENSION", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN",
        I."FTC_VIGENCIA",I."FTC_GENERACION", I."FTB_BONO", I."FTC_TIPO_PENSION", I."FTC_PERFIL_INVERSION",
        F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",F."FCN_ID_GENERACION",
        'CANDADO' AS "FTC_CANDADO_APERTURA",F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
        F."FCN_ID_INDICADOR_CLIENTE",F."FCN_ID_INDICADOR_AFILIACION",
        F."FCN_ID_INDICADOR_BONO",F."FCN_ID_PERIODICIDAD_GENERACION",
        F."FCN_ID_PERIODICIDAD_ANVERSO",F."FCN_ID_PERIODICIDAD_REVERSO",
        FE."FTC_DESCRIPCION_CORTA" AS "FTC_TIPOGENERACION",
        FE."FTC_DESC_GENERACION" AS "FTC_DESC_TIPOGENERACION",
        FE."FTC_DESCRIPCION" AS "FTC_FORMATO",:user AS "FTC_USUARIO_ALTA",
        CAST(:end as TIMESTAMP) AS "FTD_FECHA_CORTE",
        CAST(:start - INTERVAL '1 month' * (PA."FTN_MESES" - 1) AS TIMESTAMP) AS "FTD_FECHA_GRAL_INICIO",
        CAST(:end as TIMESTAMP)  AS "FTD_FECHA_GRAL_FIN",
        CAST(:start as TIMESTAMP)  - INTERVAL '1 month' * (PR."FTN_MESES" - 1) AS "FTD_FECHA_MOV_INICIO",
        CAST(:end as TIMESTAMP)  AS "FTD_FECHA_MOV_FIN",
        IEC."FTC_DESCRIPCION" AS "FTC_TIPO_TRABAJADOR"
        FROM "HECHOS"."TCHECHOS_CLIENTE" I
        INNER JOIN "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        ON
         F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
           WHEN 'AFORE' THEN 2
           WHEN 'TRANSICION' THEN 3
           WHEN 'MIXTO' THEN 4
        END
        AND  I."FCN_ID_PERIODO" = :term
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
                ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_CLIENTE"
                AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
                   WHEN 'ISSSTE' THEN  67
                   WHEN 'IMSS' THEN 66
                   WHEN 'MIXTO' THEN 69
                   WHEN 'INDEPENDIENTE' THEN 68
                END
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
                ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_AFILIACION"
                AND IEC."FTN_VALOR" = CASE
                   WHEN I."FTB_PENSION" THEN 1
                   WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
                   WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
                END
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
                ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_BONO"
                AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
                   WHEN false THEN 1
                   WHEN true THEN 2
                END
        WHERE I."FTC_TIPO_CLIENTE" <> 'DECIMO TRANSITORIO' AND
              mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
                AND F."FTB_ESTATUS" = true AND I."FCN_CUENTA" IN {cuentas}
        UNION ALL
        SELECT
        DISTINCT
        I."FCN_CUENTA", I."FCN_ID_PERIODO", I."FTB_PENSION", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN",
        I."FTC_VIGENCIA",I."FTC_GENERACION", I."FTB_BONO", I."FTC_TIPO_PENSION", I."FTC_PERFIL_INVERSION",
        F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",F."FCN_ID_GENERACION",
        'CANDADO' AS "FTC_CANDADO_APERTURA",F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
        F."FCN_ID_INDICADOR_CLIENTE",F."FCN_ID_INDICADOR_AFILIACION",
        F."FCN_ID_INDICADOR_BONO",F."FCN_ID_PERIODICIDAD_GENERACION",
        F."FCN_ID_PERIODICIDAD_ANVERSO",F."FCN_ID_PERIODICIDAD_REVERSO",
        NULL "FTC_TIPOGENERACION",
        NULL "FTC_DESC_TIPOGENERACION",
        NULL "FTC_FORMATO",
        :user AS "FTC_USUARIO_ALTA",
        CAST(:end as TIMESTAMP) AS "FTD_FECHA_CORTE",
        CAST(:start - INTERVAL '1 month' * (PA."FTN_MESES" - 1) AS TIMESTAMP) AS "FTD_FECHA_GRAL_INICIO",
        CAST(:end as TIMESTAMP)  AS "FTD_FECHA_GRAL_FIN",
        CAST(:start as TIMESTAMP)  - INTERVAL '1 month' * (PR."FTN_MESES" - 1) AS "FTD_FECHA_MOV_INICIO",
        CAST(:end as TIMESTAMP)  AS "FTD_FECHA_MOV_FIN",
        NULL AS "FTC_TIPO_TRABAJADOR"
        FROM "HECHOS"."TCHECHOS_CLIENTE" I
        INNER JOIN "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        ON
         F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
            WHEN 'DECIMO TRANSITORIO' THEN 1
        END
        AND  I."FCN_ID_PERIODO" = :term
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
        WHERE I."FTC_TIPO_CLIENTE" <> 'DECIMO TRANSITORIO' AND
        mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
        AND F."FTB_ESTATUS" = true AND I."FCN_CUENTA" IN {cuentas}
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        print("DATASET")
        dataset_df.show()

        dataset_df.write.format("parquet").partitionBy("FTC_GENERACION","FTN_ID_FORMATO").mode("overwrite").save(
            f"gs://{bucket_name}/{prefix_definitivos}/dataset.parquet")

        clientes_maestros_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT
        DISTINCT
        "FTN_CUENTA" as "FCN_CUENTA", "FTC_NOMBRE", "FTC_AP_PATERNO", "FTC_AP_MATERNO",
        "FTC_CALLE", "FTC_NUMERO", "FTC_COLONIA", 
        concat_ws(' ',"FTC_DELEGACION", "FTC_MUNICIPIO") as "FTC_DELEGACION", "FTN_CODIGO_POSTAL",
        "FTC_ENTIDAD_FEDERATIVA", "FTC_NSS",
        "FTC_RFC", "FTC_CURP", "FTC_MUNICIPIO",
        "FTC_CORREO", "FTC_TELEFONO"
        FROM "MAESTROS"."TCDATMAE_CLIENTE"
        where "FTN_CUENTA" IN {cuentas} 
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        clientes_pension_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT 
        "FCN_CUENTA", "FTC_TIPO_PENSION", "FTN_MONTO_PEN"
        FROM "MAESTROS"."TCDATMAE_PENSION"
        """, {"term": term_id})

        clientes_pension_df.write.format("parquet").partitionBy("FTC_TIPO_PENSION").mode("overwrite").save(
            f"gs://{bucket_name}/{prefix_definitivos}/TCDATMAE_PENSION.parquet")
        print("clientes_pension_df")

        indicador_cuenta_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT 
        "FTN_ID_INDICADOR_ESTADO_CUENTA", "FTN_INDICADOR", 
        "FTN_VALOR","FTC_DESCRIPCION", "FCN_ID_TIPO_INDICADOR"
        FROM "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" 
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        print("indicador_cuenta_df")

        periodos_reverso_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_REVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        print("periodos_reverso_df")

        periodos_anverso_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        print("periodos_anverso_df")

        periodos_anverso_cuatrimestral_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT DISTINCT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_REVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :end - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
                """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        print("periodos_anverso_cuatrimestral_df")

        peridicidad_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
                SELECT 
                "FTN_ID_PERIODICIDAD", "FTC_DESCRIPCION", "FTC_DESCRIPCION_CORTA", "FTN_MESES"
                FROM "GESTOR"."TCGESPRO_PERIODICIDAD"
                """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        print("peridicidad_df")

        peridicidad_df.createOrReplaceTempView("TCGESPRO_PERIODICIDAD")
        periodos_reverso_df.createOrReplaceTempView("periodos_reverso")
        periodos_anverso_df.createOrReplaceTempView("periodos_anverso")
        periodos_anverso_cuatrimestral_df.createOrReplaceTempView("periodos_anverso_cuatrimestral")
        clientes_maestros_df = clientes_maestros_df.repartition("FTC_ENTIDAD_FEDERATIVA","FTC_MUNICIPIO")
        clientes_maestros_df.createOrReplaceTempView("TCDATMAE_CLIENTE")
        indicador_cuenta_df.createOrReplaceTempView("TCGESPRO_INDICADOR_ESTADO_CUENTA")

        dataset_df = spark.sql(f"""
        SELECT
        DISTINCT
        I.FCN_CUENTA, I.FCN_ID_PERIODO, I.FTB_PENSION, I.FTC_TIPO_CLIENTE, I.FTC_ORIGEN,
        I.FTC_VIGENCIA,I.FTC_GENERACION, I.FTB_BONO, I.FTC_TIPO_PENSION, I.FTC_PERFIL_INVERSION,
        I.FTN_ID_GRUPO_SEGMENTACION,I.FCN_ID_GENERACION,I.FTC_CANDADO_APERTURA,I.FTN_ID_FORMATO,
        I.FCN_ID_INDICADOR_CLIENTE,I.FCN_ID_INDICADOR_AFILIACION,
        I.FCN_ID_INDICADOR_BONO,I.FTC_TIPOGENERACION,
        I.FTC_DESC_TIPOGENERACION,I.FTC_FORMATO,I.FTC_USUARIO_ALTA,
        I.FTD_FECHA_GRAL_INICIO,I.FTD_FECHA_GRAL_FIN,
        I.FTD_FECHA_MOV_INICIO,I.FTD_FECHA_MOV_FIN,I.FCN_ID_PERIODICIDAD_GENERACION,
        I.FCN_ID_PERIODICIDAD_ANVERSO, I.FCN_ID_PERIODICIDAD_REVERSO,
        I.FTC_TIPO_TRABAJADOR, I.FTD_FECHA_CORTE,TP.FTN_MONTO_PEN AS FTN_PENSION_MENSUAL
        FROM 
        parquet. `gs://{bucket_name}/{prefix_definitivos}/dataset.parquet` I
        LEFT JOIN  
        parquet. `gs://{bucket_name}/{prefix_definitivos}/TCDATMAE_PENSION.parquet` TP
        ON TP.FCN_CUENTA = I.FCN_CUENTA 
        WHERE I.FCN_CUENTA IN {cuentas}
        """)

        dataset_df = dataset_df.repartition("FCN_ID_GENERACION","FTN_ID_FORMATO")
        dataset_df.createOrReplaceTempView("DATASET")

        spark.sql("""select * from DATASET """).show()

        print("Query General Inicio")

        general_df = spark.sql("""
        SELECT
        DISTINCT
        D.FTN_ID_GRUPO_SEGMENTACION, D.FTC_CANDADO_APERTURA, D.FTN_ID_FORMATO,
        D.FCN_ID_PERIODO, D.FCN_CUENTA AS FCN_NUMERO_CUENTA, D.FTD_FECHA_CORTE,
        D.FTD_FECHA_GRAL_INICIO, D.FTD_FECHA_GRAL_FIN, D.FTD_FECHA_MOV_INICIO,
        D.FTD_FECHA_MOV_FIN,  0 as FTN_ID_SIEFORE, D.FTC_PERFIL_INVERSION AS FTC_DESC_SIEFORE,
        D.FTC_TIPOGENERACION, D.FTC_DESC_TIPOGENERACION,
        concat_ws(' ', C.FTC_AP_PATERNO, C.FTC_AP_MATERNO, C.FTC_NOMBRE) AS FTC_NOMBRE_COMPLETO,
        concat_ws(' ', C.FTC_CALLE, C.FTC_NUMERO) AS FTC_CALLE_NUMERO,
        C.FTC_COLONIA, C.FTC_DELEGACION, C.FTN_CODIGO_POSTAL AS FTN_CP,
        C.FTC_ENTIDAD_FEDERATIVA, C.FTC_NSS, C.FTC_RFC, C.FTC_CURP, D.FTC_TIPO_PENSION AS FTC_TIPO_PENSION,
        D.FTN_PENSION_MENSUAL, D.FTC_FORMATO, D.FTC_TIPO_TRABAJADOR, D.FTC_USUARIO_ALTA
        FROM DATASET D
        INNER JOIN TCDATMAE_CLIENTE C ON D.FCN_CUENTA = C.FCN_CUENTA
        """)

        # Generación de columnas FCN_ID_EDOCTA y FCN_FOLIO
        general_df = general_df.withColumn("FCN_ID_EDOCTA", F.concat_ws("",
                                                                        F.col("FCN_NUMERO_CUENTA"),
                                                                        F.col("FCN_ID_PERIODO"),
                                                                        F.col("FTN_ID_FORMATO")).cast("bigint"))

        general_df = general_df.withColumn("consecutivo", row_number().over(Window.orderBy(lit(0))).cast("string"))
        # Fill the "consecutivo" column with 9-digit values
        general_df = general_df.withColumn("consecutivo", lpad("consecutivo", 9, '0'))
        general_df = general_df.withColumn("FCN_FOLIO", concat(
            lit(random),
            col("FCN_ID_PERIODO"),
            col("FTN_ID_FORMATO"),
            col("consecutivo"),
        ))
        general_df = general_df.drop(col("consecutivo"))

        general_df = general_df.repartition("FTN_ID_FORMATO", "FTC_DESC_SIEFORE")

        # Mostrar información de columnas generadas
        print("FCN_ID_EDOCTA")
        print("FCN_FOLIO")

        mov_profuturo_consar_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT 
        "FTN_ID_MOV_PROFUTURO_CONSAR", "FCN_ID_MOVIMIENTO_CONSAR", "FCN_ID_MOVIMIENTO_PROFUTURO", "FCN_MONPES"
        FROM "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        print("mov_profuturo_consar_df")
        mov_profuturo_consar_df.createOrReplaceTempView("TTGESPRO_MOV_PROFUTURO_CONSAR")

        ref_mov_consar_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT 
        "FTN_ID_REFERENCIA", "FTC_TABLA_CAMPO", "FTB_FIJA", "FTC_REFERENCIA_VARIABLE", "FTC_FORMATO"
        FROM "GESTOR"."TCGESPRO_REFER_MOV_CONSAR"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        print("ref_mov_consar_df")
        ref_mov_consar_df.createOrReplaceTempView("TCGESPRO_REFER_MOV_CONSAR")

        mov_consar_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT 
        "FTN_ID_MOVIMIENTO_CONSAR", "FTC_DESCRIPCION", "FTC_MOV_TIPO_AHORRO", 
        "FTB_INTEGRACION_DIAS_COTIZADOS_SALARIO_BASE", "FCN_ID_REFERENCIA"
        FROM "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        print("mov_consar_df")
        mov_consar_df.createOrReplaceTempView("TCDATMAE_MOVIMIENTO_CONSAR")

        reverso_df = spark.sql(f"""
        SELECT
        DISTINCT
        F.FTN_ID_FORMATO,
        MC.FTC_MOV_TIPO_AHORRO AS FTC_SECCION,
        R.FCN_CUENTA AS FCN_NUMERO_CUENTA,
        R.FTD_FEH_LIQUIDACION AS FTD_FECHA_MOVIMIENTO,
        MC.FTN_ID_MOVIMIENTO_CONSAR AS FTN_ID_CONCEPTO,
        MC.FTC_DESCRIPCION AS FTC_DESC_CONCEPTO,
        CASE
        WHEN TRMC.FTB_FIJA = true THEN TRMC.FTC_REFERENCIA_VARIABLE
        WHEN TRMC.FTB_FIJA = false AND TRMC.FTC_REFERENCIA_VARIABLE LIKE '%R.F.C. <RFC PATRÓN>1%' THEN concat(R.FCN_ID_PERIODO,'-',R.FTC_SUA_RFC_PATRON)
        WHEN TRMC.FTB_FIJA = false AND TRMC.FTC_REFERENCIA_VARIABLE LIKE '%IMSS/ISSSTE%' THEN concat(R.FCN_ID_PERIODO,'-',R.TIPO_SUBCUENA)
        ELSE ''
        END FTC_PERIODO_REFERENCIA,
        R.FTF_MONTO_PESOS AS FTN_MONTO,
        CASE
        WHEN MC.FTB_INTEGRACION_DIAS_COTIZADOS_SALARIO_BASE THEN R.FTN_SUA_DIAS_COTZDOS_BIMESTRE
        END FTN_DIA_COTIZADO,
        CASE
        WHEN MC.FTB_INTEGRACION_DIAS_COTIZADOS_SALARIO_BASE THEN R.FTN_SUA_ULTIMO_SALARIO_INT_PER
        END FTN_SALARIO_BASE,
        now() AS FTD_FECHAHORA_ALTA,
        F.FTC_USUARIO_ALTA
        FROM DATASET F
        INNER JOIN TCGESPRO_PERIODICIDAD PG ON F.FCN_ID_PERIODICIDAD_REVERSO = PG.FTN_ID_PERIODICIDAD
        INNER JOIN TTHECHOS_MOVIMIENTO R ON F.FCN_CUENTA = R.FCN_CUENTA
        INNER JOIN periodos_reverso 
        ON R.FCN_ID_PERIODO BETWEEN periodos_reverso.PERIODO_INICIAL AND periodos_reverso.PERIODO_FINAL
        AND periodos_reverso.FCN_ID_FORMATO_ESTADO_CUENTA = F.FTN_ID_FORMATO
        INNER JOIN TTGESPRO_MOV_PROFUTURO_CONSAR PC ON R.FCN_ID_CONCEPTO_MOVIMIENTO = PC.FCN_ID_MOVIMIENTO_PROFUTURO
        AND PC.FCN_MONPES = R.FTN_MONPES AND PC.FCN_ID_MOVIMIENTO_CONSAR NOT IN (2,6,5,29,11,3)
        INNER JOIN TCDATMAE_MOVIMIENTO_CONSAR MC ON PC.FCN_ID_MOVIMIENTO_CONSAR = MC.FTN_ID_MOVIMIENTO_CONSAR
        LEFT JOIN TCGESPRO_REFER_MOV_CONSAR  TRMC ON MC.FCN_ID_REFERENCIA = TRMC.FTN_ID_REFERENCIA
        UNION ALL
        SELECT 
        FTN_ID_FORMATO,
        FTC_SECCION,
        FCN_NUMERO_CUENTA,
        FTD_FECHA_MOVIMIENTO,
        FTN_ID_CONCEPTO,
        FTC_DESC_CONCEPTO,
        FTC_PERIODO_REFERENCIA,
        SUM(CAST(FTF_MONTO_PESOS AS numeric(16,2))) AS FTN_MONTO,
        FTN_DIA_COTIZADO,
        FTN_SALARIO_BASE,
        FTD_FECHAHORA_ALTA,
        FTC_USUARIO_ALTA
        FROM(
        SELECT
        DISTINCT
        F.FTN_ID_FORMATO,
        MC.FTC_MOV_TIPO_AHORRO AS FTC_SECCION,
        R.FCN_CUENTA AS FCN_NUMERO_CUENTA,
        NULL AS FTD_FECHA_MOVIMIENTO,
        MC.FTN_ID_MOVIMIENTO_CONSAR AS FTN_ID_CONCEPTO,
        MC.FTC_DESCRIPCION AS FTC_DESC_CONCEPTO,
        NULL AS FTC_PERIODO_REFERENCIA,
        R.FTF_MONTO_PESOS,
        NULL AS FTN_DIA_COTIZADO,
        NULL AS FTN_SALARIO_BASE,
        now() AS FTD_FECHAHORA_ALTA,
        F.FTC_USUARIO_ALTA
        FROM DATASET F
        INNER JOIN TCGESPRO_PERIODICIDAD PG ON F.FCN_ID_PERIODICIDAD_REVERSO = PG.FTN_ID_PERIODICIDAD
        INNER JOIN TTHECHOS_MOVIMIENTO R ON F.FCN_CUENTA = R.FCN_CUENTA
        INNER JOIN periodos_reverso 
        ON R.FCN_ID_PERIODO BETWEEN periodos_reverso.PERIODO_INICIAL AND periodos_reverso.PERIODO_FINAL
        AND periodos_reverso.FCN_ID_FORMATO_ESTADO_CUENTA = F.FTN_ID_FORMATO
        INNER JOIN TTGESPRO_MOV_PROFUTURO_CONSAR PC ON R.FCN_ID_CONCEPTO_MOVIMIENTO = PC.FCN_ID_MOVIMIENTO_PROFUTURO
        AND PC.FCN_MONPES = R.FTN_MONPES AND PC.FCN_ID_MOVIMIENTO_CONSAR IN (2,6,5,29,11,3)
        INNER JOIN TCDATMAE_MOVIMIENTO_CONSAR MC ON PC.FCN_ID_MOVIMIENTO_CONSAR = MC.FTN_ID_MOVIMIENTO_CONSAR
        LEFT JOIN TCGESPRO_REFER_MOV_CONSAR  TRMC ON MC.FCN_ID_REFERENCIA = TRMC.FTN_ID_REFERENCIA
        )
        GROUP BY
        FTN_ID_FORMATO,
        FTC_SECCION,
        FCN_NUMERO_CUENTA,
        FTD_FECHA_MOVIMIENTO,
        FTN_ID_CONCEPTO,
        FTC_DESC_CONCEPTO,
        FTC_PERIODO_REFERENCIA,
        FTN_DIA_COTIZADO,
        FTN_SALARIO_BASE,
        FTD_FECHAHORA_ALTA,
        FTC_USUARIO_ALTA
        """)
        print(reverso_df.count())
        reverso_df.show()

        reverso_df = reverso_df.repartition("FTN_ID_FORMATO", "FTC_SECCION", "FTN_ID_CONCEPTO")

        configuracion_anverso_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
                SELECT 
                "FCN_GENERACION", "FTC_AHORRO", "FTA_SUBCUENTAS", "FTC_DES_CONCEPTO", "FTC_SECCION", "FTN_ORDEN_SDO"
                FROM "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO"
                """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        print("configuracion_anverso_df")
        configuracion_anverso_df.createOrReplaceTempView("TCGESPRO_CONFIGURACION_ANVERSO")

        periodo_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
                SELECT 
                "FTN_ID_PERIODO", "FTC_PERIODO"
                FROM "GESTOR"."TCGESPRO_PERIODO"
                """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        print("periodo_df")
        periodo_df.createOrReplaceTempView("TCGESPRO_PERIODO")

        bono_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT 
        DISTINCT 
        "FCN_CUENTA", "FTF_BON_NOM_ACC", "FTF_BON_NOM_PES", "FTF_BON_ACT_ACC", 
        "FTF_BON_ACT_PES", "FCN_ID_PERIODO", "FTD_FEC_RED_BONO", "FTN_FACTOR"
        FROM "HECHOS"."TTCALCUL_BONO"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        bono_df.createOrReplaceTempView("TTCALCUL_BONO")
        print("bono_df")

        print("anverso inicio")

        anverso_df = spark.sql(f"""
        SELECT
        F.FCN_CUENTA FCN_NUMERO_CUENTA,
        F.FTN_ID_FORMATO,
        C.FTC_DES_CONCEPTO AS FTC_CONCEPTO_NEGOCIO,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' THEN R.FTF_ABONO ELSE 0 END AS FTF_APORTACION,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' THEN R.FTF_CARGO ELSE 0 END AS FTN_RETIRO,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' THEN R.FTF_COMISION ELSE 0 END AS FTN_COMISION,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' AND R.FCN_ID_PERIODO = periodos_anverso.PERIODO_INICIAL THEN R.FTF_SALDO_FINAL ELSE 0 END AS FTN_SALDO_ANTERIOR,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND R.FCN_ID_PERIODO = periodos_anverso.PERIODO_FINAL THEN R.FTF_SALDO_FINAL ELSE 0 END AS FTN_SALDO_FINAL,
        C.FTC_SECCION,
        C.FTC_AHORRO AS FTC_TIPO_AHORRO,
        C.FTN_ORDEN_SDO AS FTN_ORDEN_SDO,
        F.FTC_USUARIO_ALTA,
        null AS FTN_VALOR_ACTUAL_PESO,
        null AS FTN_VALOR_ACTUAL_UDI,
        null AS FTN_VALOR_NOMINAL_PESO,
        null AS FTN_VALOR_NOMINAL_UDI
        FROM DATASET F
        INNER JOIN TCGESPRO_CONFIGURACION_ANVERSO C ON F.FCN_ID_GENERACION = C.FCN_GENERACION
        INNER JOIN TCGESPRO_PERIODICIDAD P ON F.FCN_ID_PERIODICIDAD_GENERACION = P.FTN_ID_PERIODICIDAD
        INNER JOIN TCGESPRO_PERIODICIDAD PG ON F.FCN_ID_PERIODICIDAD_ANVERSO = PG.FTN_ID_PERIODICIDAD
        INNER JOIN TTCALCUL_RENDIMIENTO R ON F.FCN_CUENTA = R.FCN_CUENTA
        INNER JOIN periodos_anverso 
        ON R.FCN_ID_PERIODO BETWEEN periodos_anverso.PERIODO_INICIAL AND periodos_anverso.PERIODO_FINAL
        AND periodos_anverso.FCN_ID_FORMATO_ESTADO_CUENTA = F.FTN_ID_FORMATO
        INNER JOIN TCGESPRO_PERIODO T ON R.FCN_ID_PERIODO = T.FTN_ID_PERIODO
        INNER JOIN TCGESPRO_PERIODO PR ON PR.FTN_ID_PERIODO = F.FCN_ID_PERIODO
        """)

        anverso_df.show(80)

        anverso_df = anverso_df.repartition("FTC_SECCION", "FTC_TIPO_AHORRO","FTN_ORDEN_SDO")

        anverso_df = anverso_df.groupBy(
            "FCN_NUMERO_CUENTA",
            "FTN_ID_FORMATO",
            "FTC_CONCEPTO_NEGOCIO",
            "FTC_SECCION",
            "FTC_TIPO_AHORRO",
            "FTN_ORDEN_SDO",
            "FTC_USUARIO_ALTA",
            "FTN_VALOR_ACTUAL_PESO",
            "FTN_VALOR_ACTUAL_UDI",
            "FTN_VALOR_NOMINAL_PESO",
            "FTN_VALOR_NOMINAL_UDI"
        ).agg(
            F.sum("FTF_APORTACION").alias("FTF_APORTACION"),
            F.sum("FTN_RETIRO").alias("FTN_RETIRO"),
            F.sum("FTN_COMISION").alias("FTN_COMISION"),
            F.sum("FTN_SALDO_ANTERIOR").alias("FTN_SALDO_ANTERIOR"),
            F.sum("FTN_SALDO_FINAL").alias("FTN_SALDO_FINAL")
        )

        anverso_df.show(80)

        bono_df = spark.sql("""
        SELECT
        TCB.FCN_CUENTA AS FCN_NUMERO_CUENTA,
        D.FTN_ID_FORMATO,
        NULL AS FTC_CONCEPTO_NEGOCIO,
        'BON' AS FTC_SECCION,
        NULL AS FTC_TIPO_AHORRO,
        NULL AS FTN_ORDEN_SDO,
        D.FTC_USUARIO_ALTA,
        TCB.FTF_BON_ACT_PES AS FTN_VALOR_ACTUAL_PESO,
        TCB.FTF_BON_ACT_ACC AS FTN_VALOR_ACTUAL_UDI,
        TCB.FTF_BON_NOM_PES AS FTN_VALOR_NOMINAL_PESO,
        TCB.FTF_BON_NOM_ACC AS FTN_VALOR_NOMINAL_UDI,
        NULL AS FTF_APORTACION,
        NULL AS FTN_RETIRO,
        NULL AS FTN_COMISION,
        NULL AS FTN_SALDO_ANTERIOR,
        NULL AS FTN_SALDO_FINAL,
        NULL AS FTN_RENDIMIENTO
        FROM TTCALCUL_BONO TCB
        INNER JOIN DATASET D ON D.FCN_CUENTA = TCB.FCN_CUENTA
        WHERE TCB.FCN_ID_PERIODO = D.FCN_ID_PERIODO
        """)
        # Repartition the DataFrame based on the specified columns

        anverso_df = anverso_df.withColumn("FTN_RENDIMIENTO", col("FTN_SALDO_FINAL") - (
                    col("FTF_APORTACION") + col("FTN_SALDO_ANTERIOR") - col("FTN_COMISION") - col("FTN_RETIRO")))

        anverso_df = anverso_df.union(bono_df)

        anverso_df = anverso_df.repartition("FTC_SECCION", "FTC_TIPO_AHORRO", "FTN_ORDEN_SDO")

        print("anverso cuatrimestral inicio")

        anverso_cuatrimestral_df = spark.sql(f"""
        SELECT
        DISTINCT
        F.FCN_CUENTA FCN_NUMERO_CUENTA,
        F.FTN_ID_FORMATO,
        C.FTC_DES_CONCEPTO AS FTC_CONCEPTO_NEGOCIO,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' THEN R.FTF_ABONO ELSE 0 END AS FTF_APORTACION,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' THEN R.FTF_CARGO ELSE 0 END AS FTN_RETIRO,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' THEN R.FTF_COMISION ELSE 0 END AS FTN_COMISION,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND C.FTC_SECCION <> 'SDO' AND R.FCN_ID_PERIODO = periodos_anverso_cuatrimestral.PERIODO_INICIAL THEN R.FTF_SALDO_FINAL ELSE 0 END AS FTN_SALDO_ANTERIOR,
        CASE WHEN array_contains(C.FTA_SUBCUENTAS, R.FCN_ID_TIPO_SUBCTA) AND R.FCN_ID_PERIODO = periodos_anverso_cuatrimestral.PERIODO_FINAL THEN R.FTF_SALDO_FINAL ELSE 0 END AS FTN_SALDO_FINAL,
        C.FTC_SECCION,
        C.FTC_AHORRO AS FTC_TIPO_AHORRO,
        C.FTN_ORDEN_SDO AS FTN_ORDEN_SDO,
        F.FTC_USUARIO_ALTA
        FROM DATASET F
        INNER JOIN TCGESPRO_CONFIGURACION_ANVERSO C ON F.FCN_ID_GENERACION = C.FCN_GENERACION
        INNER JOIN TCGESPRO_PERIODICIDAD P ON F.FCN_ID_PERIODICIDAD_GENERACION = P.FTN_ID_PERIODICIDAD
        INNER JOIN TCGESPRO_PERIODICIDAD PG ON F.FCN_ID_PERIODICIDAD_ANVERSO = PG.FTN_ID_PERIODICIDAD
        INNER JOIN TTCALCUL_RENDIMIENTO R ON F.FCN_CUENTA = R.FCN_CUENTA
        INNER JOIN periodos_anverso_cuatrimestral 
        ON R.FCN_ID_PERIODO BETWEEN periodos_anverso_cuatrimestral.PERIODO_INICIAL AND periodos_anverso_cuatrimestral.PERIODO_FINAL
        AND periodos_anverso_cuatrimestral.FCN_ID_FORMATO_ESTADO_CUENTA = F.FTN_ID_FORMATO
        INNER JOIN TCGESPRO_PERIODO T ON R.FCN_ID_PERIODO = T.FTN_ID_PERIODO
        INNER JOIN TCGESPRO_PERIODO PR ON PR.FTN_ID_PERIODO = F.FCN_ID_PERIODO
        WHERE FTC_SECCION NOT IN ('SDO') AND FTC_AHORRO NOT IN ('VIV')
        """)

        anverso_cuatrimestral_df = anverso_cuatrimestral_df.repartition("FTC_SECCION", "FTC_TIPO_AHORRO", "FTN_ORDEN_SDO")

        anverso_cuatrimestral_df = anverso_cuatrimestral_df.groupBy(
            "FCN_NUMERO_CUENTA",
            "FTN_ID_FORMATO",
            "FTC_CONCEPTO_NEGOCIO",
            "FTC_SECCION",
            "FTC_TIPO_AHORRO",
            "FTN_ORDEN_SDO",
            "FTC_USUARIO_ALTA"
        ).agg(
            F.sum("FTF_APORTACION").alias("FTF_APORTACION"),
            F.sum("FTN_RETIRO").alias("FTN_RETIRO"),
            F.sum("FTN_COMISION").alias("FTN_COMISION"),
            F.sum("FTN_SALDO_ANTERIOR").alias("FTN_SALDO_ANTERIOR"),
            F.sum("FTN_SALDO_FINAL").alias("FTN_SALDO_FINAL")
        )

        anverso_cuatrimestral_df = anverso_cuatrimestral_df.withColumn("FTN_RENDIMIENTO", col("FTN_SALDO_FINAL") - (
                col("FTF_APORTACION") + col("FTN_SALDO_ANTERIOR") - col("FTN_COMISION") - col("FTN_RETIRO")))

        anverso_cuatrimestral_df = anverso_cuatrimestral_df.repartition("FTC_SECCION", "FTC_TIPO_AHORRO",
                                                                        "FTN_ORDEN_SDO")

        general_df.createOrReplaceTempView("general")
        anverso_cuatrimestral_df.createOrReplaceTempView("anverso_cuatrimestral")
        anverso_df.createOrReplaceTempView("anverso")
        reverso_df.createOrReplaceTempView("reverso")


        general_df = spark.sql(f"""
        SELECT 
        C.FTN_ID_GRUPO_SEGMENTACION,C.FTC_CANDADO_APERTURA,C.FTN_ID_FORMATO,C.FCN_ID_PERIODO,
        cast(C.FCN_NUMERO_CUENTA as bigint) FCN_NUMERO_CUENTA,C.FTD_FECHA_CORTE,C.FTD_FECHA_GRAL_INICIO,C.FTD_FECHA_GRAL_FIN,C.FTD_FECHA_MOV_INICIO,
        C.FTD_FECHA_MOV_FIN,C.FTN_ID_SIEFORE,C.FTC_DESC_SIEFORE,C.FTC_TIPOGENERACION,C.FTC_DESC_TIPOGENERACION,
        C.FTC_NOMBRE_COMPLETO,C.FTC_CALLE_NUMERO,C.FTC_COLONIA,C.FTC_DELEGACION,C.FTN_CP,C.FTC_ENTIDAD_FEDERATIVA,
        C.FTC_NSS,C.FTC_RFC,C.FTC_CURP,C.FTC_TIPO_PENSION,C.FTN_PENSION_MENSUAL,C.FTC_FORMATO,C.FTC_TIPO_TRABAJADOR,
        C.FTC_USUARIO_ALTA,
        C.FCN_ID_EDOCTA,
        C.FCN_FOLIO, COALESCE(ASS.FTN_MONTO,0) AS FTN_SALDO_SUBTOTAL, COALESCE(AST.FTN_MONTO,0) AS FTN_SALDO_TOTAL
        FROM general C
            LEFT JOIN (
                SELECT FCN_NUMERO_CUENTA, FTN_ID_FORMATO, SUM(Cast(FTN_SALDO_FINAL as numeric(16,2))) AS FTN_MONTO
                FROM anverso
                WHERE FTC_SECCION = 'AHO'
                GROUP BY FCN_NUMERO_CUENTA, FTN_ID_FORMATO
            ) ASS ON C.FCN_NUMERO_CUENTA = ASS.FCN_NUMERO_CUENTA
            LEFT JOIN (
                SELECT FCN_NUMERO_CUENTA,FTN_ID_FORMATO, SUM(Cast(FTN_SALDO_FINAL as numeric(16,2))) AS FTN_MONTO
                FROM anverso
                 WHERE FTC_SECCION NOT IN ('SDO')
                GROUP BY FCN_NUMERO_CUENTA,FTN_ID_FORMATO
            ) AST ON C.FCN_NUMERO_CUENTA = AST.FCN_NUMERO_CUENTA
        """)
        reverso_df = spark.sql("""
                SELECT
                DISTINCT
                cast(G.FCN_ID_EDOCTA as bigint) as FCN_ID_EDOCTA,
                cast(R.FCN_NUMERO_CUENTA as bigint) as FCN_NUMERO_CUENTA,
                R.FTN_ID_CONCEPTO,
                R.FTC_DESC_CONCEPTO,
                R.FTC_SECCION,
                R.FTD_FECHA_MOVIMIENTO,
                R.FTN_SALARIO_BASE,
                R.FTN_DIA_COTIZADO,
                R.FTC_PERIODO_REFERENCIA,
                R.FTN_MONTO,
                R.FTD_FECHAHORA_ALTA,
                R.FTC_USUARIO_ALTA, 
                R.FTN_ID_FORMATO
                FROM reverso R
                    INNER JOIN general G ON G.FCN_NUMERO_CUENTA = R.FCN_NUMERO_CUENTA AND G.FTN_ID_FORMATO = R.FTN_ID_FORMATO
                """)

        anverso_cuatrimestral_df = spark.sql("""
                SELECT
                cast(G.FCN_ID_EDOCTA as bigint) as FCN_ID_EDOCTA,
                cast(G.FCN_NUMERO_CUENTA as bigint) as FCN_NUMERO_CUENTA,
                null as FTN_ID_CONCEPTO,
                "Comisión del Periodo" as FTC_DESC_CONCEPTO,
                cast(A.FTC_TIPO_AHORRO as varchar(10)) as FTC_SECCION,
                null as FTD_FECHA_MOVIMIENTO,
                null as FTN_SALARIO_BASE,
                null as FTN_DIA_COTIZADO,
                "Profuturo"  as FTC_PERIODO_REFERENCIA,
                SUM(cast(A.FTN_COMISION as numeric(16,2))) as FTN_MONTO,
                now() AS FTD_FECHAHORA_ALTA,
                cast(A.FTC_USUARIO_ALTA as varchar(10)) as FTC_USUARIO_ALTA,
                A.FTN_ID_FORMATO    
                FROM anverso_cuatrimestral A
                INNER JOIN general G ON G.FCN_NUMERO_CUENTA = A.FCN_NUMERO_CUENTA AND G.FTN_ID_FORMATO = A.FTN_ID_FORMATO
                WHERE A.FTC_TIPO_AHORRO IN ('RET', 'VOL')
                GROUP BY 
                G.FCN_ID_EDOCTA,
                A.FTN_ID_FORMATO,
                G.FCN_NUMERO_CUENTA,
                A.FTC_TIPO_AHORRO,
                A.FTC_USUARIO_ALTA
                UNION ALL
                SELECT 
                cast(G.FCN_ID_EDOCTA as bigint) as FCN_ID_EDOCTA,
                cast(G.FCN_NUMERO_CUENTA as bigint) as FCN_NUMERO_CUENTA,
                null as FTN_ID_CONCEPTO,
                "Rendimiento del Periodo" as FTC_DESC_CONCEPTO,
                cast(A.FTC_TIPO_AHORRO as varchar(10)) as FTC_SECCION,
                null as FTD_FECHA_MOVIMIENTO,
                null as FTN_SALARIO_BASE,
                null as FTN_DIA_COTIZADO,
                "Profuturo"  as FTC_PERIODO_REFERENCIA,
                SUM(cast(A.FTN_RENDIMIENTO as numeric(16,2))) as FTN_MONTO,
                now() AS FTD_FECHAHORA_ALTA,
                cast(A.FTC_USUARIO_ALTA as varchar(10)) as FTC_USUARIO_ALTA,
                A.FTN_ID_FORMATO
                FROM anverso_cuatrimestral A
                INNER JOIN general G ON G.FCN_NUMERO_CUENTA = A.FCN_NUMERO_CUENTA AND G.FTN_ID_FORMATO = A.FTN_ID_FORMATO
                WHERE A.FTC_TIPO_AHORRO IN ('RET','VOL')
                GROUP BY 
                G.FCN_ID_EDOCTA,
                A.FTN_ID_FORMATO,
                G.FCN_NUMERO_CUENTA,
                A.FTC_TIPO_AHORRO,
                A.FTC_USUARIO_ALTA
                """)

        spark.catalog.dropTempView("reverso")
        reverso_df = reverso_df.union(anverso_cuatrimestral_df)

        reverso_df = reverso_df.repartition("FTN_ID_FORMATO", "FTC_SECCION", "FTN_ID_CONCEPTO")

        reverso_df.createOrReplaceTempView("reverso")

        reverso_df = spark.sql("""
        SELECT
        ROW_NUMBER() OVER (
            partition by
                R.FCN_NUMERO_CUENTA,
                R.FTN_ID_FORMATO,
                R.FTC_SECCION
            ORDER BY
                CASE WHEN R.FTC_DESC_CONCEPTO LIKE 'Interés%' THEN 1
                     WHEN R.FTC_DESC_CONCEPTO LIKE 'Intereses%' THEN 2
                     WHEN R.FTC_DESC_CONCEPTO LIKE 'Comisión%' THEN 3
                     WHEN R.FTC_DESC_CONCEPTO LIKE 'Rendimiento%' THEN 4
                     ELSE 5
                END,
                R.FTD_FECHA_MOVIMIENTO ASC
        ) AS FTN_ORDEN,
        R.FCN_ID_EDOCTA,
        R.FCN_NUMERO_CUENTA,
        R.FTN_ID_CONCEPTO,
        R.FTC_DESC_CONCEPTO,
        R.FTC_SECCION,
        R.FTD_FECHA_MOVIMIENTO,
        R.FTN_SALARIO_BASE,
        R.FTN_DIA_COTIZADO,
        R.FTC_PERIODO_REFERENCIA,
        R.FTN_MONTO,
        R.FTD_FECHAHORA_ALTA,
        R.FTC_USUARIO_ALTA
        FROM reverso R
        WHERE R.FTN_MONTO <> 0
        """)

        anverso_df = spark.sql("""
        SELECT 
        cast(A.FCN_NUMERO_CUENTA as bigint) FCN_NUMERO_CUENTA,A.FTC_CONCEPTO_NEGOCIO,
        A.FTF_APORTACION, A.FTN_RETIRO,A.FTN_COMISION,A.FTN_SALDO_ANTERIOR,
        A.FTN_SALDO_FINAL,A.FTC_SECCION,A.FTN_VALOR_ACTUAL_PESO,
        A.FTN_VALOR_ACTUAL_UDI,A.FTN_VALOR_NOMINAL_PESO,
        A.FTN_VALOR_NOMINAL_UDI,A.FTC_TIPO_AHORRO,A.FTN_ORDEN_SDO,
        A.FTC_USUARIO_ALTA,A.FTN_RENDIMIENTO, cast(G.FCN_ID_EDOCTA as bigint) FCN_ID_EDOCTA
        FROM anverso A
        INNER JOIN general G ON G.FCN_NUMERO_CUENTA = A.FCN_NUMERO_CUENTA AND G.FTN_ID_FORMATO = A.FTN_ID_FORMATO
        """)

        #join_reverso_df = reverso_df.join(cuatri_anversi, "FCN_ID_EDOCTA", "left")

        # List of views to destroy
        views_to_destroy = ["periodos_reverso", "periodos_anverso", "TCGESPRO_INDICADOR_ESTADO_CUENTA",
                            "DATASET", "TTCALCUL_BONO", "anverso", "general", "reverso"]

        # Destroy (drop) multiple temporary views
        for view in views_to_destroy:
            spark.catalog.dropTempView(view)

        reverso_df = reverso_df.repartition("FTC_SECCION", "FTN_ID_CONCEPTO","FTD_FECHA_MOVIMIENTO")
        general_df = general_df.repartition("FTN_ID_FORMATO","FTC_ENTIDAD_FEDERATIVA","FTC_DESC_SIEFORE")
        anverso_df = anverso_df.repartition("FTC_SECCION","FTC_TIPO_AHORRO","FTN_ORDEN_SDO")
        print("reverso, write")
        #_write_spark_dataframe(reverso_df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_REVERSO"')
        print("general, write")
        #_write_spark_dataframe(general_df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_GENERAL"')
        print("anverso, write ")
        #_write_spark_dataframe(anverso_df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_ANVERSO"')


        for i in range(1800):
            response = requests.get(url)
            print(response)
            # Verifica si la petición fue exitosa
            if response.status_code == 200:
                # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(48)
                break
            else:
                # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        cursor = postgres.execute(text("""
                SELECT
                "FTN_ID_PERIODO"
                FROM (
                SELECT
                row_number() over (ORDER BY "FTN_ID_PERIODO" DESC) rownumberId,
                "FTN_ID_PERIODO"
                FROM
                "GESTOR"."TCGESPRO_PERIODO"
                WHERE "FTN_ID_PERIODO" < :term
                ) X
                where rownumberId = 4

                """), {'term': term_id})
        cuatrimestre_anterior = cursor.fetchone()[0]
        print(cuatrimestre_anterior)

        cursor = postgres.execute(text("""
                SELECT
                "FTN_ID_PERIODO"
                FROM (
                SELECT
                row_number() over (ORDER BY "FTN_ID_PERIODO" DESC) rownumberId,
                "FTN_ID_PERIODO"
                FROM
                "GESTOR"."TCGESPRO_PERIODO"
                WHERE "FTN_ID_PERIODO" < :term
                ) X
                where rownumberId >= 12
                        """), {'term': term_id})
        anio_anterior = cursor.fetchone()[0]
        print(anio_anterior)

        delete_all_objects(bucket_name, term_id)
        source_bucket_name = f"{bucket_name}"
        destination_coldline = f"{bucket_coldline}"

        bucket_anio_anterior = f"{bucket_coldline}"
        destination_archive = f"{bucket_archive}"

        move_files_parallel(source_bucket_name, source_bucket_name, source_prefix="profuturo-archivos",destination_prefix=str(term_id))
        delete_all_objects(bucket_name, 'profuturo-archivos')
        move_files_parallel(source_bucket_name, destination_coldline, source_prefix=str(cuatrimestre_anterior),destination_prefix=str(cuatrimestre_anterior))
        delete_all_objects(bucket_name, cuatrimestre_anterior)
        move_files_parallel(bucket_anio_anterior, destination_archive, source_prefix=str(anio_anterior),destination_prefix=str(anio_anterior))

        general_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
                SELECT
                "FCN_FOLIO",
                "FCN_NUMERO_CUENTA",
                "FCN_ID_PERIODO",
                TP."FTN_MESES" as "FTN_PERIODICIDAD_MESES",
                now() as "FTD_FECHA_ALTA",
                CASE
                WHEN "FCN_ID_PERIODO" IN (202301,202302,202303,202304) THEN 83
                WHEN "FCN_ID_PERIODO" IN (202305,202306,202307,202308) THEN 84
                WHEN "FCN_ID_PERIODO" IN (202309,202310,202311,202312) THEN 85
                WHEN "FCN_ID_PERIODO" IN (202201,202202,202203,202204) THEN 74
                WHEN "FCN_ID_PERIODO" IN (202205,202206,202207,202208) THEN 75
                WHEN "FCN_ID_PERIODO" IN (202209,202210,202211,202212) THEN 76
                END "FCN_ID_PERIODO_EDOCTA"
                FROM "ESTADO_CUENTA"."TTEDOCTA_GENERAL" G
                INNER JOIN "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" TCFEC
                ON g."FTN_ID_FORMATO" = TCFEC."FCN_ID_FORMATO_ESTADO_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" TP
                ON TP."FTN_ID_PERIODICIDAD" = TCFEC."FCN_ID_PERIODICIDAD_GENERACION"
                """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        general_df = general_df.withColumn("FTC_URL_EDOCTA", concat(
            lit('https://storage.cloud.google.com/'),
            lit(f"{bucket_name}/"),
            lit(f"{term_id}/"),
            col("FCN_FOLIO"),
            lit(".pdf")
        ))

        #_write_spark_dataframe(general_df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_URL"')

        notify(
            postgres,
            "Generacion finales",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar los estados de cuenta finales con éxito",
            aprobar=False,
            descarga=False,
        )
