from sqlalchemy.engine import CursorResult
from profuturo.common import define_extraction, register_time, notify, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool,configure_mitedocta_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, _get_spark_session, _create_spark_dataframe
from pyspark.sql.functions import concat, col, row_number, lit, lpad
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.window import Window
from sqlalchemy import text
from google.cloud import storage, bigquery
from io import BytesIO
from PIL import Image
from profuturo.env import load_env
import pandas as pd
import requests
import sys
import random
import string
import time
import os
import json

load_env()
postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
storage_client = storage.Client()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
bucket_name = os.getenv("BUCKET_ID")
print(bucket_name)
prefix =f"{os.getenv('PREFIX_BLOB')}"
print(prefix)
url = os.getenv("URL_MUESTRAS_RECA")
print(url)



################### OBTENCIÓN DE MUESTRAS #######################################
def find_samples(samples_cursor: CursorResult):
    samples = set()
    i = 0
    for batch in samples_cursor.partitions(50):
        for record in batch:
            account = record[0]
            consars = record[1]
            i += 1

            for consar in consars:
                if consar not in configurations:
                    continue

                configurations[consar] = configurations[consar] - 1

                if configurations[consar] == 0:
                    del configurations[consar]

                samples.add(account)

                if len(configurations) == 0:
                    print("Cantidad de registros: " + str(i))
                    return samples

    result = tuple(list(configurations.keys()))
    print(result)

    descripcion_consar = postgres.execute(text("""
        SELECT "FTC_DESCRIPCION" FROM "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR"
        WHERE "FTN_ID_MOVIMIENTO_CONSAR" IN :consar
        """), {'consar': result}).fetchall()

    print(descripcion_consar)

    # Creating a DataFrame
    df = pd.DataFrame(descripcion_consar, columns=['Concepto Consar'])  # Replace 'Column_Name' with an appropriate column name
    html_table = df.to_html()

    notify(
        postgres,
        'No se encontraron suficientes registros para los Conceptos CONSAR',
        phase,
        area,
        term_id,
        message=f"No se encontraron suficientes muestras para los Conceptos CONSAR:",
        details=html_table,
        aprobar=False,
        descarga=False,
        reproceso=False,
    )

    return samples

def upload_to_gcs(row):
    id_value = row["id"]
    bytea_data = row["fto_imagen"]

    # Convertir bytes a imagen
    image = Image.open(BytesIO(bytea_data))

    # Guardar imagen localmente (opcional)
    # image.save(f"local/{id_value}.png")

    # Subir imagen a GCS
    blob_name = f"{prefix}/{id_value}.png"

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


with define_extraction(phase, area, postgres_pool, bigquery_pool) as (postgres, bigquery):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 100)
    spark.conf.set("spark.default.parallelism", 100)

    with register_time(postgres_pool, phase, term_id, user, area):

        filter_reprocessed_samples = ''
        is_reprocess = postgres.execute(text("""
        SELECT EXISTS(
            SELECT 1
            FROM "GESTOR"."TCGESPRO_MUESTRA"
            WHERE "FCN_ID_PERIODO" = :term
              AND "FCN_ID_AREA" = :area
              AND "FTC_ESTATUS" = 'Reprocesado'
        )
        """), {'term': term_id, "area": area}).fetchone()[0]

        print("Reprocess:", is_reprocess)
        if not is_reprocess:
            truncate_table(postgres, "TCGESPRO_MUESTRA_SOL_RE_CONSAR")
            truncate_table(postgres, "TCGESPRO_MUESTRA", term=term_id, area=area)

            cursor = postgres.execute(text("""
            SELECT "FCN_ID_MOVIMIENTO_CONSAR", "FTN_CANTIDAD"
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_MUESTRA_AUTOMATICA" WHERE "FTB_VIGENTE" = TRUE
            """))
            configurations = {str(configuration[0]): configuration[1] for configuration in cursor.fetchall()}
            print("configuracion", configurations)

            cursor = postgres.execute(text("""
            SELECT "FCN_CUENTA", array_agg(CC."FCN_ID_MOVIMIENTO_CONSAR")::varchar[]
            FROM (
                SELECT M."FCN_CUENTA", PC."FCN_ID_MOVIMIENTO_CONSAR"
                FROM "HECHOS"."TTHECHOS_MOVIMIENTO" M
                    INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON M."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
                WHERE M."FTD_FEH_LIQUIDACION" between :end - INTERVAL '4 MONTH' and :end
                GROUP BY M."FCN_CUENTA", PC."FCN_ID_MOVIMIENTO_CONSAR"
            ) AS CC
            INNER JOIN "GESTOR"."TTGESPRO_CONFIGURACION_MUESTRA_AUTOMATICA" MC ON CC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FCN_ID_MOVIMIENTO_CONSAR" AND "FTB_VIGENTE" = TRUE
            WHERE "FCN_CUENTA" IN (SELECT "FCN_CUENTA" FROM "MAESTROS"."TCDATMAE_CLIENTE")
            GROUP BY "FCN_CUENTA"
            ORDER BY sum(1.0 / "FTN_CANTIDAD") DESC
            """), {'end': end_month})
            samples = find_samples(cursor)

            print(samples)

            postgres.execute(text("""
            INSERT INTO "GESTOR"."TCGESPRO_MUESTRA" ("FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_USUARIO", "FCN_ID_AREA", "FTD_FECHAHORA_ALTA") 
            SELECT DISTINCT HC."FCN_CUENTA", :term, CA."FTC_USUARIO_CARGA"::INT, :area, current_timestamp
            FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" HC ON CA."FCN_CUENTA" = HC."FCN_CUENTA"
            WHERE "FCN_ID_INDICADOR" = 32 AND "FCN_ID_AREA" = :area
              AND CA."FCN_ID_PERIODO" = :term
            """), {"term": term_id, "area": area})
            print("muestra manuales")
        else:
            filter_reprocessed_samples = 'INNER JOIN "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR" MR ON M."FTN_ID_MUESTRA" = MR."FCN_ID_MUESTRA"'

        truncate_table(postgres, 'TTMUESTR_REVERSO')
        truncate_table(postgres, 'TTMUESTR_ANVERSO')
        truncate_table(postgres, 'TTMUESTR_GENERAL')

        ###########################  IMAGENES   #################################################
        truncate_table(postgres, 'TTEDOCTA_IMAGEN')

        delete_all_objects(bucket_name, prefix)
        delete_all_objects(bucket_name, 'profuturo-archivos')

        query = """
                SELECT
                DISTINCT
                concat("FTC_CODIGO_POSICION_PDF",'-',tcie."FCN_ID_FORMATO_ESTADO_CUENTA",'-', ra."FCN_ID_AREA",'-',COALESCE(tcie."FTC_DESCRIPCION_SIEFORE",'sinsiefore')) AS ID,"FTO_IMAGEN" AS FTO_IMAGEN
                FROM "GESTOR"."TTGESPRO_CONFIG_IMAGEN_EDOCTA" tcie
                INNER JOIN "GESTOR"."TTGESPRO_ROL_USUARIO" ru ON CAST(tcie."FTC_USUARIO" AS INT) = ru."FCN_ID_USUARIO"
                INNER JOIN "GESTOR"."TCGESPRO_ROL_AREA" ra ON ru."FCN_ID_ROL" =  ra."FCN_ID_ROL"
                """

        imagenes_df = _create_spark_dataframe(spark, configure_postgres_spark, query,params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

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
        char1 = random.choice(string.ascii_letters).upper()
        char2 = random.choice(string.ascii_letters).upper()
        random = char1 + char2
        print(random)
        general_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH DATASET AS (
            SELECT
            DISTINCT F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",F."FCN_ID_GENERACION",
            'CANDADO' AS "FTC_CANDADO_APERTURA",F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
            M."FCN_ID_PERIODO",M."FCN_CUENTA",F."FCN_ID_INDICADOR_CLIENTE",F."FCN_ID_INDICADOR_AFILIACION",
            F."FCN_ID_INDICADOR_BONO",CAST(:end as TIMESTAMP) AS "FTD_FECHA_CORTE",
            CAST(:start - INTERVAL '1 month' * (PA."FTN_MESES" - 1) AS TIMESTAMP) AS "FTD_FECHA_GRAL_INICIO",
            CAST(:end as TIMESTAMP)  AS "FTD_FECHA_GRAL_FIN",
            CAST(:start as TIMESTAMP)  - INTERVAL '1 month' * (PR."FTN_MESES" - 1) AS "FTD_FECHA_MOV_INICIO",
            CAST(:end as TIMESTAMP)  AS "FTD_FECHA_MOV_FIN",
            0 AS "FTN_ID_SIEFORE", FE."FTC_DESCRIPCION_CORTA" AS "FTC_TIPOGENERACION",
            FE."FTC_DESC_GENERACION" AS "FTC_DESC_TIPOGENERACION",TP."FTN_MONTO_PEN" AS "FTN_PENSION_MENSUAL",
            FE."FTC_DESCRIPCION" AS "FTC_FORMATO",:user AS "FTC_USUARIO_ALTA"
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
            INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
            INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M ON P."FTN_ID_PERIODO" = M."FCN_ID_PERIODO"
            LEFT JOIN "MAESTROS"."TCDATMAE_PENSION" TP
            ON TP."FCN_CUENTA" = M."FCN_CUENTA"
            {filter_reprocessed_samples}
            WHERE mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
            AND F."FTB_ESTATUS" = true
            )
            SELECT
            DISTINCT D."FTN_ID_GRUPO_SEGMENTACION", D."FTC_CANDADO_APERTURA", D."FTN_ID_FORMATO",
            D."FCN_ID_PERIODO", C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA", D."FTD_FECHA_CORTE",
            D."FTD_FECHA_GRAL_INICIO", D."FTD_FECHA_GRAL_FIN",D."FTD_FECHA_MOV_INICIO",
            D."FTD_FECHA_MOV_FIN",D."FTN_ID_SIEFORE",I."FTC_PERFIL_INVERSION" AS "FTC_DESC_SIEFORE",
            D."FTC_TIPOGENERACION",D."FTC_DESC_TIPOGENERACION",
            concat_ws(' ',C."FTC_AP_PATERNO", C."FTC_AP_MATERNO", C."FTC_NOMBRE") AS "FTC_NOMBRE_COMPLETO",
            concat_ws(' ',C."FTC_CALLE", C."FTC_NUMERO") AS "FTC_CALLE_NUMERO",
            C."FTC_COLONIA",C."FTC_DELEGACION", C."FTN_CODIGO_POSTAL" AS "FTN_CP",
            C."FTC_ENTIDAD_FEDERATIVA",C."FTC_NSS",C."FTC_RFC",C."FTC_CURP",I."FTC_TIPO_PENSION" AS "FTC_TIPO_PENSION",
            D."FTN_PENSION_MENSUAL",
            D."FTC_FORMATO",
            IEC."FTC_DESCRIPCION" AS "FTC_TIPO_TRABAJADOR",
            :user AS "FTC_USUARIO_ALTA"
            FROM DATASET D
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
            ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
            AND D."FCN_CUENTA" = I."FCN_CUENTA"
            AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
               WHEN 'AFORE' THEN 2
               WHEN 'TRANSICION' THEN 3
               WHEN 'MIXTO' THEN 4
            END
            INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
            ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_CLIENTE"
            AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
               WHEN 'ISSSTE' THEN  67
               WHEN 'IMSS' THEN 66
               WHEN 'MIXTO' THEN 69
               WHEN 'INDEPENDIENTE' THEN 68
            END
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
            ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_AFILIACION"
            AND IEC."FTN_VALOR" = CASE
               WHEN I."FTB_PENSION" THEN 1
               WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
               WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
            END
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
            ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_BONO"
            AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
               WHEN false THEN 1
               WHEN true THEN 2
            END
            UNION ALL
            SELECT
            DISTINCT D."FTN_ID_GRUPO_SEGMENTACION", D."FTC_CANDADO_APERTURA", D."FTN_ID_FORMATO",
            D."FCN_ID_PERIODO", C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA", D."FTD_FECHA_CORTE",
            D."FTD_FECHA_GRAL_INICIO", D."FTD_FECHA_GRAL_FIN",D."FTD_FECHA_MOV_INICIO",
            D."FTD_FECHA_MOV_FIN",D."FTN_ID_SIEFORE",I."FTC_PERFIL_INVERSION" AS "FTC_DESC_SIEFORE",
            D."FTC_TIPOGENERACION",D."FTC_DESC_TIPOGENERACION",
            concat_ws(' ',C."FTC_AP_PATERNO", C."FTC_AP_MATERNO", C."FTC_NOMBRE") AS "FTC_NOMBRE_COMPLETO",
            concat_ws(' ',C."FTC_CALLE", C."FTC_NUMERO") AS "FTC_CALLE_NUMERO",
            C."FTC_COLONIA",C."FTC_DELEGACION", C."FTN_CODIGO_POSTAL" AS "FTN_CP",
            C."FTC_ENTIDAD_FEDERATIVA",C."FTC_NSS",C."FTC_RFC",C."FTC_CURP",I."FTC_TIPO_PENSION" AS "FTC_TIPO_PENSION",
            D."FTN_PENSION_MENSUAL",
            D."FTC_FORMATO",
            null AS "FTC_TIPO_TRABAJADOR",
            :user AS "FTC_USUARIO_ALTA"
            FROM DATASET D
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
            ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
            AND D."FCN_CUENTA" = I."FCN_CUENTA"
            AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
               WHEN 'DECIMO TRANSITORIO' THEN 1
            END
            INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        general_df = general_df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("FCN_ID_PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))

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
        general_df.write.format("parquet").mode("overwrite").save(f"gs://{bucket_name}/{prefix}/general.parquet")
        general_df = spark.read.format("parquet").load(f"gs://{bucket_name}/{prefix}/general.parquet").repartition(10)
        general_df.createOrReplaceTempView("general")
        spark.sql("select FCN_FOLIO from general").show()

        reverso_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH data as (
        SELECT
        F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO", M."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
        M."FCN_ID_PERIODO",M."FCN_CUENTA", F."FCN_ID_GENERACION", F."FCN_ID_INDICADOR_CLIENTE",F."FCN_ID_INDICADOR_AFILIACION",
        F."FCN_ID_INDICADOR_BONO"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
        INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M ON P."FTN_ID_PERIODO" = M."FCN_ID_PERIODO"
        {filter_reprocessed_samples}
        -- QUITAR COMENT  CONDICION VALIDA PERO NO ES PERIODO CUATRIMESTRAL
        WHERE mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
        AND F."FTB_ESTATUS" = true
        ), dataset as (
        SELECT
        D."FTN_ID_FORMATO", D."FCN_NUMERO_CUENTA", concat(C."FTC_NSS",'-',C."FTC_RFC") AS "FTC_NSS_RFC"
        FROM data D
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
        ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
        AND D."FCN_CUENTA" = I."FCN_CUENTA"
        AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
           WHEN 'AFORE' THEN 2
           WHEN 'TRANSICION' THEN 3
           WHEN 'MIXTO' THEN 4 END
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
        ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_CLIENTE"
        AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
           WHEN 'ISSSTE' THEN 67
           WHEN 'IMSS' THEN 66
           WHEN 'MIXTO' THEN 69
           WHEN 'INDEPENDIENTE' THEN 68 END
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
        ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_AFILIACION"
        AND IEC."FTN_VALOR" = CASE
           WHEN I."FTB_PENSION" THEN 1
           WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
           WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
        END
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
        ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_BONO"
        AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
           WHEN false THEN  1
           WHEN true THEN 2 END
        UNION ALL
        SELECT
        DISTINCT D."FTN_ID_FORMATO", D."FCN_NUMERO_CUENTA", concat(C."FTC_NSS",'-',C."FTC_RFC") AS "FTC_NSS_RFC"
        FROM data D
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
        ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
        AND D."FCN_CUENTA" = I."FCN_CUENTA"
        AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
            WHEN 'DECIMO TRANSITORIO' THEN 1
        END
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
        )
        , periodos AS (
        SELECT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_REVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :end - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
        ), movimientos as (
        SELECT
        DISTINCT
        R."FCN_CUENTA",
        R."FCN_ID_PERIODO",
        R."FTF_MONTO_PESOS",
        R."FTN_SUA_DIAS_COTZDOS_BIMESTRE",
        R."FTN_SUA_ULTIMO_SALARIO_INT_PER",
        R."FTD_FEH_LIQUIDACION",
        R."FCN_ID_CONCEPTO_MOVIMIENTO",
        R."FTC_SUA_RFC_PATRON",
        SB."FTC_TIPO_CLIENTE" AS "TIPO_SUBCUENA",
        0 AS "FTN_MONPES"
        FROM "HECHOS"."TTHECHOS_MOVIMIENTO" R
        INNER JOIN dataset M
        ON M."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
        INNER JOIN periodos ON R."FCN_ID_PERIODO" BETWEEN periodos.PERIODO_INICIAL AND periodos.PERIODO_FINAL AND M."FTN_ID_FORMATO" = periodos."FCN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" SB
        ON SB."FTN_ID_TIPO_SUBCTA" = R."FCN_ID_TIPO_SUBCTA"
        UNION ALL
        SELECT
        R."CSIE1_NUMCUE",
        R."FCN_ID_PERIODO",
        R."MONTO" AS "FTF_MONTO_PESOS",
        NULL AS "FTN_SUA_DIAS_COTZDOS_BIMESTRE",
        NULL AS "FTN_SUA_ULTIMO_SALARIO_INT_PER",
        to_date(cast(R."CSIE1_FECCON" as varchar),'YYYYMMDD') AS "FTD_FEH_LIQUIDACION",
        CAST(R."CSIE1_CODMOV"AS INT) AS "FCN_ID_CONCEPTO_MOVIMIENTO",
        NULL AS "FTC_SUA_RFC_PATRON",
        SB."FTC_TIPO_CLIENTE" AS "TIPO_SUBCUENA",
        MP."FTN_MONPES"
        FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY" R
        INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" MP
        ON R."SUBCUENTA" = MP."FCN_ID_TIPO_SUBCUENTA" AND CAST(R."CSIE1_CODMOV" AS INT) = MP."FTN_ID_MOVIMIENTO_PROFUTURO"
        INNER JOIN dataset M
        ON M."FCN_NUMERO_CUENTA" = R."CSIE1_NUMCUE"
        INNER JOIN periodos ON R."FCN_ID_PERIODO" BETWEEN periodos.PERIODO_INICIAL AND periodos.PERIODO_FINAL AND M."FTN_ID_FORMATO" = periodos."FCN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" SB
        ON SB."FTN_ID_TIPO_SUBCTA" = R."SUBCUENTA"
        )
        SELECT
        DISTINCT
        F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
        MC."FTC_MOV_TIPO_AHORRO" AS "FTC_SECCION",
        R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
        R."FTD_FEH_LIQUIDACION" AS "FTD_FECHA_MOVIMIENTO",
        MC."FTN_ID_MOVIMIENTO_CONSAR" AS "FTN_ID_CONCEPTO",
        MC."FTC_DESCRIPCION" AS "FTC_DESC_CONCEPTO",
        CASE
        WHEN TRMC."FTB_FIJA" = true THEN TRMC."FTC_REFERENCIA_VARIABLE"
        WHEN TRMC."FTB_FIJA" = false AND TRMC."FTC_REFERENCIA_VARIABLE" LIKE '%R.F.C. <RFC PATRÓN>1%' THEN concat(R."FCN_ID_PERIODO",'-',R."FTC_SUA_RFC_PATRON")
        WHEN TRMC."FTB_FIJA" = false AND TRMC."FTC_REFERENCIA_VARIABLE" LIKE '%IMSS/ISSSTE%' THEN concat(R."FCN_ID_PERIODO",'-',R."TIPO_SUBCUENA")
        WHEN TRMC."FTB_FIJA" = false AND TRMC."FTC_REFERENCIA_VARIABLE" LIKE '%NSS-CURP%' THEN concat(R."FCN_ID_PERIODO",'-',D."FTC_NSS_RFC")
        END "FTC_PERIODO_REFERENCIA",
        R."FTF_MONTO_PESOS" AS "FTN_MONTO",
        CASE
        WHEN MC."FTB_INTEGRACION_DIAS_COTIZADOS_SALARIO_BASE" THEN R."FTN_SUA_DIAS_COTZDOS_BIMESTRE"
        END "FTN_DIA_COTIZADO",
        CASE
        WHEN MC."FTB_INTEGRACION_DIAS_COTIZADOS_SALARIO_BASE" THEN R."FTN_SUA_ULTIMO_SALARIO_INT_PER"
        END "FTN_SALARIO_BASE",
        now() AS "FTD_FECHAHORA_ALTA",
        :user AS "FTC_USUARIO_ALTA"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_REVERSO" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN dataset D ON D."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN movimientos R ON D."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
        INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
        AND PC."FCN_MONPES" = R."FTN_MONPES" AND PC."FCN_ID_MOVIMIENTO_CONSAR" NOT IN (2,6,5,29,11,3)
        INNER JOIN "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FTN_ID_MOVIMIENTO_CONSAR"
        LEFT JOIN "GESTOR"."TCGESPRO_REFER_MOV_CONSAR"  TRMC ON MC."FCN_ID_REFERENCIA" = TRMC."FTN_ID_REFERENCIA"
        UNION ALL
        SELECT
        F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
        MC."FTC_MOV_TIPO_AHORRO" AS "FTC_SECCION",
        R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
        NULL AS "FTD_FECHA_MOVIMIENTO",
        MC."FTN_ID_MOVIMIENTO_CONSAR" AS "FTN_ID_CONCEPTO",
        MC."FTC_DESCRIPCION" AS "FTC_DESC_CONCEPTO",
        CAST(NULL AS varchar) AS "FTC_PERIODO_REFERENCIA",
        sum(CAST(R."FTF_MONTO_PESOS" AS numeric(16,2))) AS "FTN_MONTO",
        NULL AS "FTN_DIA_COTIZADO",
        NULL AS "FTN_SALARIO_BASE",
        now() AS "FTD_FECHAHORA_ALTA",
        :user AS "FTC_USUARIO_ALTA"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_REVERSO" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN dataset D ON D."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN movimientos R ON D."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
        INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
        AND PC."FCN_MONPES" = R."FTN_MONPES" AND PC."FCN_ID_MOVIMIENTO_CONSAR" IN (2,6,5,29,11,3)
        INNER JOIN "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FTN_ID_MOVIMIENTO_CONSAR" AND PC."FCN_MONPES" = R."FTN_MONPES"
        LEFT JOIN "GESTOR"."TCGESPRO_REFER_MOV_CONSAR"  TRMC ON MC."FCN_ID_REFERENCIA" = TRMC."FTN_ID_REFERENCIA"
        GROUP BY
        F."FCN_ID_FORMATO_ESTADO_CUENTA",
        MC."FTC_MOV_TIPO_AHORRO",
        R."FCN_CUENTA",
        MC."FTN_ID_MOVIMIENTO_CONSAR",
        MC."FTC_DESCRIPCION"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        reverso_df.write.format("parquet").mode("overwrite").save(f"gs://{bucket_name}/{prefix}/reverso.parquet")
        reverso_df = spark.read.format("parquet").load(f"gs://{bucket_name}/{prefix}/reverso.parquet").repartition(10)
        reverso_df.createOrReplaceTempView("reverso")

        anverso_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH data as (
        SELECT
        DISTINCT F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO", M."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
        M."FCN_ID_PERIODO",M."FCN_CUENTA", F."FCN_ID_GENERACION", F."FCN_ID_INDICADOR_CLIENTE",F."FCN_ID_INDICADOR_AFILIACION",
        F."FCN_ID_INDICADOR_BONO"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
        INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M ON P."FTN_ID_PERIODO" = M."FCN_ID_PERIODO"
        {filter_reprocessed_samples}
        -- QUITAR COMENT  CONDICION VALIDA PERO NO ES PERIODO CUATRIMESTRAL
        WHERE mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
        AND F."FTB_ESTATUS" = true
        ), dataset as (
        SELECT
        D."FTN_ID_FORMATO", D."FCN_NUMERO_CUENTA"
        FROM data D
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
        ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
        AND D."FCN_CUENTA" = I."FCN_CUENTA"
        AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
           WHEN 'AFORE' THEN 2
           WHEN 'TRANSICION' THEN 3
           WHEN 'MIXTO' THEN 4 END
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
        ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_CLIENTE"
        AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
           WHEN 'ISSSTE' THEN 67
           WHEN 'IMSS' THEN 66
           WHEN 'MIXTO' THEN 69
           WHEN 'INDEPENDIENTE' THEN 68 END
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
        ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_AFILIACION"
        AND IEC."FTN_VALOR" = CASE
           WHEN I."FTB_PENSION" THEN 1
           WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
           WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
        END
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
        ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_BONO"
        AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
           WHEN false THEN  1
           WHEN true THEN 2 END
        UNION ALL
        SELECT
        DISTINCT D."FTN_ID_FORMATO", D."FCN_NUMERO_CUENTA"
        FROM data D
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
        ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
        AND D."FCN_CUENTA" = I."FCN_CUENTA"
        AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
            WHEN 'DECIMO TRANSITORIO' THEN 1
        END
        )
        , periodos AS (
        SELECT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
            INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
        )
        SELECT
        "FCN_NUMERO_CUENTA",
        "FTN_ID_FORMATO",
        "FTC_CONCEPTO_NEGOCIO",
        SUM("FTF_APORTACION") AS "FTF_APORTACION",
        SUM("FTN_RETIRO") AS "FTN_RETIRO",
        SUM("FTN_COMISION") AS "FTN_COMISION",
        SUM("FTN_SALDO_ANTERIOR") AS "FTN_SALDO_ANTERIOR",
        SUM("FTN_SALDO_FINAL") AS "FTN_SALDO_FINAL",
        "FTC_SECCION",
        NULL ::numeric(16, 2) AS  "FTN_VALOR_ACTUAL_PESO",
        NULL ::numeric(16, 6) AS "FTN_VALOR_ACTUAL_UDI",
        NULL ::numeric(16, 2) AS "FTN_VALOR_NOMINAL_PESO",
        NULL ::numeric(16, 6) AS "FTN_VALOR_NOMINAL_UDI",
        "FTC_TIPO_AHORRO",
        "FTN_ORDEN_SDO",
        :user AS "FTC_USUARIO_ALTA"
        FROM (
        SELECT
        D."FCN_NUMERO_CUENTA",
        D."FTN_ID_FORMATO",
        C."FTC_DES_CONCEPTO" AS "FTC_CONCEPTO_NEGOCIO",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' THEN R."FTF_ABONO" ELSE 0 END ::numeric(16, 2) AS "FTF_APORTACION",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' THEN R."FTF_CARGO" ELSE 0 END::numeric(16, 2) AS "FTN_RETIRO",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' THEN R."FTF_COMISION" ELSE 0 END::numeric(16, 2) AS "FTN_COMISION",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' AND R."FCN_ID_PERIODO" = periodos.PERIODO_INICIAL THEN R."FTF_SALDO_FINAL" ELSE 0 END::numeric(16, 2) AS "FTN_SALDO_ANTERIOR",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = periodos.PERIODO_FINAL THEN R."FTF_SALDO_FINAL" ELSE 0 END::numeric(16, 2) AS "FTN_SALDO_FINAL",
        C."FTC_SECCION",
        C."FTC_AHORRO" AS "FTC_TIPO_AHORRO",
        C."FTN_ORDEN_SDO" AS "FTN_ORDEN_SDO"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN dataset D ON D."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
        LEFT JOIN "HECHOS"."TTCALCUL_BONO" TCB ON TCB."FCN_CUENTA" = D."FCN_NUMERO_CUENTA"
        INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON D."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
        INNER JOIN periodos ON R."FCN_ID_PERIODO" BETWEEN periodos.PERIODO_INICIAL AND periodos.PERIODO_FINAL and periodos."FCN_ID_FORMATO_ESTADO_CUENTA" = D."FTN_ID_FORMATO"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR ON PR."FTN_ID_PERIODO" = :term
        --where "FTC_SECCION" <> 'SDO' and C."FTC_DES_CONCEPTO" = 'Ahorro para el retiro 92 y 97<sup>1</sup>'
        ) X
        GROUP BY
        "FCN_NUMERO_CUENTA",
        "FTN_ID_FORMATO",
        "FTC_CONCEPTO_NEGOCIO",
        "FTC_SECCION",
        "FTN_VALOR_ACTUAL_PESO",
        "FTN_VALOR_ACTUAL_UDI",
        "FTN_VALOR_NOMINAL_PESO",
        "FTN_VALOR_NOMINAL_UDI",
        "FTC_TIPO_AHORRO",
        "FTN_ORDEN_SDO",
        "FTC_USUARIO_ALTA"
        UNION ALL
        SELECT
        TCB."FCN_CUENTA" AS FCN_NUMERO_CUENTA,
        D."FTN_ID_FORMATO",
        NULL AS "FTC_CONCEPTO_NEGOCIO",
        NULL AS "FTF_APORTACION",
        NULL AS "FTN_RETIRO",
        NULL AS "FTN_COMISION",
        NULL AS "FTN_SALDO_ANTERIOR",
        NULL AS "FTN_SALDO_FINAL",
        'BON' AS "FTC_SECCION",
        TCB."FTF_BON_ACT_PES"::numeric(16, 2) AS "FTN_VALOR_ACTUAL_PESO",
        TCB."FTF_BON_ACT_ACC"::numeric(16, 6) AS "FTN_VALOR_ACTUAL_UDI",
        TCB."FTF_BON_NOM_PES"::numeric(16, 2) AS "FTN_VALOR_NOMINAL_PESO",
        TCB."FTF_BON_NOM_ACC"::numeric(16, 6) AS "FTN_VALOR_NOMINAL_UDI",
        NULL AS "FTC_TIPO_AHORRO",
        NULL AS "FTN_ORDEN_SDO",
        :user AS "FTC_USUARIO_ALTA"
        FROM "HECHOS"."TTCALCUL_BONO" TCB
        INNER JOIN dataset D ON D."FCN_NUMERO_CUENTA" = TCB."FCN_CUENTA"
        WHERE TCB."FCN_ID_PERIODO" = :term
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        anverso_df.write.format("parquet").mode("overwrite").save(f"gs://{bucket_name}/{prefix}/anverso.parquet")
        anverso_df = spark.read.format("parquet").load(f"gs://{bucket_name}/{prefix}/anverso.parquet").repartition(10)
        anverso_df = anverso_df.withColumn("FTN_RENDIMIENTO", col("FTN_SALDO_FINAL") - (col("FTF_APORTACION") + col("FTN_SALDO_ANTERIOR") - col("FTN_COMISION") - col("FTN_RETIRO")))
        anverso_df.show()
        anverso_df.createOrReplaceTempView("anverso")


        anverso_cuatrimestral_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH data as (
        SELECT
        DISTINCT F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO", M."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
        M."FCN_ID_PERIODO",M."FCN_CUENTA", F."FCN_ID_GENERACION", F."FCN_ID_INDICADOR_CLIENTE",F."FCN_ID_INDICADOR_AFILIACION",
        F."FCN_ID_INDICADOR_BONO"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
        INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M ON P."FTN_ID_PERIODO" = M."FCN_ID_PERIODO"
        {filter_reprocessed_samples}
        -- QUITAR COMENT  CONDICION VALIDA PERO NO ES PERIODO CUATRIMESTRAL
        WHERE mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
        AND F."FTB_ESTATUS" = true
        ), dataset as (
        SELECT
        D."FTN_ID_FORMATO", D."FCN_NUMERO_CUENTA"
        FROM data D
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
        ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
        AND D."FCN_CUENTA" = I."FCN_CUENTA"
        AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
        WHEN 'AFORE' THEN 2
        WHEN 'TRANSICION' THEN 3
        WHEN 'MIXTO' THEN 4 END
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
        ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_CLIENTE"
        AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
        WHEN 'ISSSTE' THEN 67
        WHEN 'IMSS' THEN 66
        WHEN 'MIXTO' THEN 69
        WHEN 'INDEPENDIENTE' THEN 68 END
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
        ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_AFILIACION"
        AND IEC."FTN_VALOR" = CASE
        WHEN I."FTB_PENSION" THEN 1
        WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
        WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
        END
        INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
        ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = D."FCN_ID_INDICADOR_BONO"
        AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
        WHEN false THEN  1
        WHEN true THEN 2 END
        UNION ALL
        SELECT
        DISTINCT D."FTN_ID_FORMATO", D."FCN_NUMERO_CUENTA"
        FROM data D
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
        ON D."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
        AND D."FCN_CUENTA" = I."FCN_CUENTA"
        AND D."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
        WHEN 'DECIMO TRANSITORIO' THEN 1
        END
        )
        , periodos AS (
       SELECT F."FCN_ID_FORMATO_ESTADO_CUENTA", min(T."FTN_ID_PERIODO") AS PERIODO_INICIAL, max(T."FTN_ID_PERIODO") AS PERIODO_FINAL
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" A ON F."FCN_ID_GENERACION" = A."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD" AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_REVERSO" = PA."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :end - INTERVAL '1 month' * PA."FTN_MESES" AND :end
        GROUP BY "FTN_ID_CONFIGURACION_FORMATO_ESTADO_CUENTA"
        )
        SELECT
        "FCN_NUMERO_CUENTA",
        "FTN_ID_FORMATO",
        "FTC_CONCEPTO_NEGOCIO",
        SUM("FTF_APORTACION") AS "FTF_APORTACION",
        SUM("FTN_RETIRO") AS "FTN_RETIRO",
        SUM("FTN_COMISION") AS "FTN_COMISION",
        SUM("FTN_SALDO_ANTERIOR") AS "FTN_SALDO_ANTERIOR",
        SUM("FTN_SALDO_FINAL") AS "FTN_SALDO_FINAL",
        "FTC_SECCION",
        "FTC_TIPO_AHORRO",
        "FTN_ORDEN_SDO",
        :user AS "FTC_USUARIO_ALTA"
        FROM (
        SELECT
        D."FCN_NUMERO_CUENTA",
        D."FTN_ID_FORMATO",
        C."FTC_DES_CONCEPTO" AS "FTC_CONCEPTO_NEGOCIO",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' THEN R."FTF_ABONO" ELSE 0 END ::numeric(16, 2) AS "FTF_APORTACION",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' THEN R."FTF_CARGO" ELSE 0 END::numeric(16, 2) AS "FTN_RETIRO",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' THEN R."FTF_COMISION" ELSE 0 END::numeric(16, 2) AS "FTN_COMISION",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND C."FTC_SECCION" <> 'SDO' AND R."FCN_ID_PERIODO" = periodos.PERIODO_INICIAL THEN R."FTF_SALDO_FINAL" ELSE 0 END::numeric(16, 2) AS "FTN_SALDO_ANTERIOR",
        CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = periodos.PERIODO_FINAL THEN R."FTF_SALDO_FINAL" ELSE 0 END::numeric(16, 2) AS "FTN_SALDO_FINAL",
        C."FTC_SECCION",
        C."FTC_AHORRO" AS "FTC_TIPO_AHORRO",
        C."FTN_ORDEN_SDO" AS "FTN_ORDEN_SDO"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PG."FTN_ID_PERIODICIDAD"
        INNER JOIN dataset D ON D."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
        LEFT JOIN "HECHOS"."TTCALCUL_BONO" TCB ON TCB."FCN_CUENTA" = D."FCN_NUMERO_CUENTA"
        INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON D."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
        INNER JOIN periodos ON R."FCN_ID_PERIODO" BETWEEN periodos.PERIODO_INICIAL AND periodos.PERIODO_FINAL and periodos."FCN_ID_FORMATO_ESTADO_CUENTA" = D."FTN_ID_FORMATO"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR ON PR."FTN_ID_PERIODO" = :term
        where "FTC_SECCION" NOT IN ('SDO') AND "FTC_AHORRO" NOT IN ('VIV')
        ) X
        GROUP BY
        "FCN_NUMERO_CUENTA",
        "FTN_ID_FORMATO",
        "FTC_CONCEPTO_NEGOCIO",
        "FTC_SECCION",
        "FTC_TIPO_AHORRO",
        "FTN_ORDEN_SDO",
        "FTC_USUARIO_ALTA"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        anverso_cuatrimestral_df.write.format("parquet").mode("overwrite").save(f"gs://{bucket_name}/{prefix}/anverso_cuatri.parquet")
        anverso_cuatrimestral_df = spark.read.format("parquet").load(f"gs://{bucket_name}/{prefix}/anverso_cuatri.parquet").repartition(10)
        anverso_cuatrimestral_df = anverso_cuatrimestral_df.withColumn("FTN_RENDIMIENTO", col("FTN_SALDO_FINAL") - (
                    col("FTF_APORTACION") + col("FTN_SALDO_ANTERIOR") - col("FTN_COMISION") - col("FTN_RETIRO")))

        anverso_cuatrimestral_df.show()
        df = anverso_cuatrimestral_df.select("FCN_NUMERO_CUENTA").show()
        anverso_cuatrimestral_df.createOrReplaceTempView("anverso_cuatrimestral")

        general_df = spark.sql("""
        SELECT
        C.FTN_ID_GRUPO_SEGMENTACION,C.FTC_CANDADO_APERTURA,C.FTN_ID_FORMATO,C.FCN_ID_PERIODO,
        cast(C.FCN_NUMERO_CUENTA as bigint) FCN_NUMERO_CUENTA,C.FTD_FECHA_CORTE,C.FTD_FECHA_GRAL_INICIO,C.FTD_FECHA_GRAL_FIN,C.FTD_FECHA_MOV_INICIO,
        C.FTD_FECHA_MOV_FIN,C.FTN_ID_SIEFORE,C.FTC_DESC_SIEFORE,C.FTC_TIPOGENERACION,C.FTC_DESC_TIPOGENERACION,
        C.FTC_NOMBRE_COMPLETO,C.FTC_CALLE_NUMERO,C.FTC_COLONIA,C.FTC_DELEGACION,C.FTN_CP,C.FTC_ENTIDAD_FEDERATIVA,
        C.FTC_NSS,C.FTC_RFC,C.FTC_CURP,C.FTC_TIPO_PENSION,C.FTN_PENSION_MENSUAL,C.FTC_FORMATO,C.FTC_TIPO_TRABAJADOR,
        C.FTC_USUARIO_ALTA,
        C.FCN_ID_EDOCTA,
        C.FCN_FOLIO,
        cast(ASS.FTN_MONTO as numeric(16,2)) AS FTN_SALDO_SUBTOTAL, cast(AST.FTN_MONTO as numeric(16,2)) AS FTN_SALDO_TOTAL
        FROM general C
            INNER JOIN (
                SELECT FCN_NUMERO_CUENTA, FTN_ID_FORMATO, SUM(cast(FTN_SALDO_FINAL as numeric(16,2))) AS FTN_MONTO
                FROM anverso
                WHERE FTC_SECCION = 'AHO'
                GROUP BY FCN_NUMERO_CUENTA, FTN_ID_FORMATO
            ) ASS ON C.FCN_NUMERO_CUENTA = ASS.FCN_NUMERO_CUENTA
            INNER JOIN (
                SELECT FCN_NUMERO_CUENTA,FTN_ID_FORMATO, SUM(cast(FTN_SALDO_FINAL as numeric(16,2))) AS FTN_MONTO
                FROM anverso
                 WHERE FTC_SECCION NOT IN ('SDO')
                GROUP BY FCN_NUMERO_CUENTA,FTN_ID_FORMATO
            ) AST ON C.FCN_NUMERO_CUENTA = AST.FCN_NUMERO_CUENTA
        -- WHERE C.FCN_ID_EDOCTA IS NOT NULL
        """).cache()
        general_df.show()

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
        WHERE R.FCN_NUMERO_CUENTA IN (SELECT FCN_NUMERO_CUENTA FROM anverso_cuatrimestral)
        """).cache()

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
                HAVING SUM(cast(A.FTN_COMISION as numeric(16,2))) <> 0
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
                WHERE A.FTC_TIPO_AHORRO IN ('RET', 'VIV','VOL')
                GROUP BY 
                G.FCN_ID_EDOCTA,
                A.FTN_ID_FORMATO,
                G.FCN_NUMERO_CUENTA,
                A.FTC_TIPO_AHORRO,
                A.FTC_USUARIO_ALTA
                HAVING SUM(cast(A.FTN_RENDIMIENTO as numeric(16,2))) <> 0
                """)

        reverso_df = reverso_df.union(anverso_cuatrimestral_df)

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
        """).cache()


        anverso_df = anverso_df.drop(col("FTN_ID_FORMATO"))
        reverso_df = reverso_df.drop(col("FTN_ID_FORMATO"))

        print("reverso")
        _write_spark_dataframe(reverso_df, configure_postgres_spark, '"ESTADO_CUENTA"."TTMUESTR_REVERSO"')
        print("anverso")
        _write_spark_dataframe(anverso_df, configure_postgres_spark, '"ESTADO_CUENTA"."TTMUESTR_ANVERSO"')
        print("general")
        _write_spark_dataframe(general_df, configure_postgres_spark, '"ESTADO_CUENTA"."TTMUESTR_GENERAL"')

        reverso_df.unpersist()
        anverso_df.unpersist()
        general_df.unpersist()

        if is_reprocess:
            postgres.execute(text("""
            UPDATE "GESTOR"."TCGESPRO_MUESTRA"
            SET "FTC_ESTATUS" = null
            WHERE "FTC_ESTATUS" = 'Reprocesado'
              AND "FCN_ID_PERIODO" = :term
              AND "FCN_ID_AREA" = :area
            """), {"term": term_id, "area": area})

        for i in range(1000):
            response = requests.get(url)
            print(response)
            # Verifica si la petición fue exitosa
            if response.status_code == 200:
                # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(8)
            else:
                # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        notify(
            postgres,
            "Generacion muestras",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar las muestras de los estados de cuenta con éxito",
            aprobar=False,
            descarga=False,
        )
