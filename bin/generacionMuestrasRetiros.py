from profuturo.common import define_extraction, register_time, truncate_table, notify
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_postgres_oci_spark,configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool
from profuturo.extraction import _write_spark_dataframe, extract_terms,  _get_spark_session, read_table_insert_temp_view, extract_dataset_spark, _create_spark_dataframe
from pyspark.sql.functions import udf, concat, col, current_date , row_number,lit, current_timestamp
from profuturo.imagen import upload_to_gcs, delete_all_objects, get_blob_info
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from datetime import datetime, timedelta
from profuturo.env import load_env
from sqlalchemy import text
from google.cloud import storage

from io import BytesIO
from PIL import Image
import sys
import requests
import time
import os
import json
import sys
import jwt

spark = _get_spark_session()
load_env()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
storage_client = storage.Client()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
url = os.getenv("URL_MUESTRAS_RET")
bucket_name = os.getenv("BUCKET_ID")
print(bucket_name)
prefix =f"{os.getenv('PREFIX_BLOB')}"
print(prefix)


def get_token():
    try:
        payload = {"isNonRepudiation": True}
        secret = os.environ.get("JWT_SECRET")  # Ensure you have set the JWT_SECRET environment variable

        if secret is None:
            raise ValueError("JWT_SECRET environment variable is not set")

        # Set expiration time 10 seconds from now
        expiration_time = datetime.utcnow() + timedelta(seconds=10)
        payload['exp'] = expiration_time.timestamp()  # Setting expiration time directly in payload

        # Create the token
        non_repudiation_token = jwt.encode(payload, secret, algorithm='HS256')

        return non_repudiation_token
    except Exception:
        return -1


def get_headers():
    non_repudiation_token = get_token()
    if non_repudiation_token != -1:
        return {"Authorization": f"Bearer {non_repudiation_token}"}
    else:
        return {}


with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):

        def insertar_tablas():
            # Extracción de tablas temporales

            extract_dataset_spark(configure_postgres_spark, configure_postgres_oci_spark,
                                  """ SELECT * FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO" """,
                                  '"HECHOS"."TTHECHOS_CARGA_ARCHIVO"'
                                  )
        def eliminar_tablas():
            # Elimina tablas temporales
            postgres_oci.execute(
                text(""" DROP TABLE IF EXISTS "HECHOS"."TTHECHOS_CARGA_ARCHIVO" """))


        #truncate_table(postgres, 'TCGESPRO_MUESTRA', term=term_id, area=area)
        postgres.execute(
            text(f""" DELETE FROM "GESTOR"."TCGESPRO_MUESTRA"
            WHERE "FCN_ID_USUARIO" = {user}
            """))
        truncate_table(postgres_oci, 'TTMUESTR_RETIRO_GENERAL')
        truncate_table(postgres_oci, 'TTMUESTR_RETIRO')
        eliminar_tablas()
        insertar_tablas()

        read_table_insert_temp_view(configure_postgres_oci_spark, """
        SELECT
        DISTINCT
        CAST(CONCAT(C."FTN_CUENTA", CAST(to_char(F."FTD_FECHA_EMISION", 'YYYYMMDD') AS BIGINT)) AS BIGINT) AS "FCN_ID_EDOCTA",
        cast(C."FTN_CUENTA" as BIGINT) AS "FCN_NUMERO_CUENTA",
        :term AS "FCN_ID_PERIODO",
        concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE",
        concat_ws(' ',C."FTC_CALLE", C."FTC_NUMERO") AS "FTC_CALLE_NUMERO",
        CASE
            WHEN C."FTC_COLONIA" LIKE '%NO ASIGNADO%' THEN C."FTC_ASENTAMIENTO"
            ELSE COALESCE(C."FTC_COLONIA",C."FTC_ASENTAMIENTO")
        END AS "FTC_COLONIA",
        COALESCE(concat_ws(' ',C."FTC_MUNICIPIO",C."FTC_DELEGACION"), C."FTC_DELEGACION") AS "FTC_MUNICIPIO",
        COALESCE(C."FTC_CODIGO_POSTAL", ' ') AS "FTC_CP",
        COALESCE(C."FTC_ENTIDAD_FEDERATIVA", ' ') AS "FTC_ENTIDAD",
        COALESCE(C."FTC_CURP", ' ') AS "FTC_CURP",
        COALESCE(C."FTC_RFC", ' ') AS "FTC_RFC",
        COALESCE(C."FTC_NSS", ' ') AS "FTC_NSS",
        :user AS "FTC_USUARIO_ALTA"
        FROM "HECHOS"."TTHECHOS_RETIRO" F
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON F."FCN_CUENTA" = C."FTN_CUENTA"
        INNER JOIN "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA ON CA."FCN_CUENTA" = C."FTN_CUENTA" AND CA."FCN_ID_INDICADOR" = 32
        AND CA."FCN_ID_AREA" = :area
        where C."FTN_CUENTA" is not null
        and ca."FTC_USUARIO_CARGA" = :user
        """, "edoCtaGenerales", params={"term": term_id, "user": str(user), "area": area})
        general_df = spark.sql("""
        select * from edoCtaGenerales
        """)

        read_table_insert_temp_view(configure_postgres_oci_spark,
          """
                SELECT
                DISTINCT
                R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
                CAST(CONCAT(R."FCN_CUENTA", CAST(to_char(R."FTD_FECHA_EMISION", 'YYYYMMDD') AS BIGINT)) AS BIGINT) AS "FCN_ID_EDOCTA",
                :term AS "FCN_ID_PERIODO",
                "FTN_SDO_INI_AHORRORET" AS "FTN_SDO_INI_AHO_RET",
                "FTN_SDO_INI_VIVIENDA" AS "FTN_SDO_INI_AHO_VIV",
                "FTN_SDO_TRA_AHORRORET" AS "FTN_SDO_TRA_AHO_RET",
                "FTN_SDO_TRA_VIVIENDA" AS "FTN_SDO_TRA_AHO_VIV",
                "FTN_SDO_INI_AHORRORET" - "FTN_SDO_TRA_AHORRORET" AS "FTN_SDO_REM_AHO_RET",
                "FTN_SDO_INI_VIVIENDA" - "FTN_SDO_TRA_VIVIENDA" AS "FTN_SDO_REM_AHO_VIV",
                "FTC_LEY_PENSION",
                "FTC_REGIMEN",
                "FTC_TPSEGURO" AS "FTC_SEGURO",
                "FTC_TPPENSION" AS "FTC_TIPO_PENSION",
                "FTC_FON_ENTIDAD",
                CASE
                WHEN "FTC_FON_ENTIDAD" is not null THEN "FTD_FECHA_EMISION"
                END "FTD_FON_FECHA_TRANSF",
                CASE
                WHEN "FTC_FON_ENTIDAD" is not null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET"
                ELSE 0
                END "FTN_FON_MONTO_TRANSF",
                0.0 AS "FTN_FON_RETENCION_ISR",
                "FTC_ENT_REC_TRAN" AS "FTC_AFO_ENTIDAD",
                "FCC_MEDIO_PAGO" AS "FTC_AFO_MEDIO_PAGO",
                CASE
                WHEN "FTC_FON_ENTIDAD" is null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET" - "FTN_AFO_ISR"
                ELSE 0
                END "FTC_AFO_RECURSOS_ENTREGA",
                CASE
                WHEN "FCC_MEDIO_PAGO" is not null then "FTD_FECHA_EMISION"
                END "FTD_AFO_FECHA_ENTREGA",
                "FTN_AFO_ISR" AS "FTC_AFO_RETENCION_ISR",
                CAST("FTN_FEH_INI_PEN" AS INT) AS "FTC_FECHA_INICIO_PENSION",
                CAST("FTN_FEH_RES_PEN" as INT) AS "FTC_FECHA_EMISION",
                'NO DISPONIBLE' as "FTN_PENSION_INSTITUTO_SEG",
                "FTC_TIPO_TRAMITE" As "FTN_TIPO_TRAMITE",
                "FTN_ARCHIVO" AS "FTC_ARCHIVO",
                "FTD_FECHA_EMISION" AS "FTD_FECHA_LIQUIDACION",
                ("FTN_SDO_INI_AHORRORET" - "FTN_SDO_TRA_AHORRORET") +  ("FTN_SDO_INI_VIVIENDA" - "FTN_SDO_TRA_VIVIENDA") AS "FTN_SALDO_FINAL",
                :user AS "FTC_USUARIO_ALTA"
                FROM "HECHOS"."TTHECHOS_RETIRO" R
                INNER JOIN "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA ON CA."FCN_CUENTA" = R."FCN_CUENTA" AND CA."FCN_ID_INDICADOR" = 32
                AND "FCN_ID_AREA" = :area
                WHERE R."FCN_ID_PERIODO" = :term
                and ca."FTC_USUARIO_CARGA" = :user
                """, "edoCtaAnverso", params={"term": term_id, "user":str(user), "area": area})

        anverso_df = spark.sql("select * from edoCtaAnverso")


        _write_spark_dataframe(general_df, configure_postgres_oci_spark, '"ESTADO_CUENTA"."TTMUESTR_RETIRO_GENERAL"')
        _write_spark_dataframe(anverso_df, configure_postgres_oci_spark, '"ESTADO_CUENTA"."TTMUESTR_RETIRO"')

        df = spark.sql(f"""
        SELECT 
         FCN_NUMERO_CUENTA AS FCN_CUENTA,
         CAST({user} as int)  FTC_USUARIO_CARGA
        FROM edoCtaGenerales
        """)
        print("muestras")
        df.show()

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))
        df = df.withColumn("FCN_ID_USUARIO", col("FTC_USUARIO_CARGA").cast("int"))
        df = df.drop("FTC_USUARIO_CARGA")
        df = df.withColumn("FCN_ID_AREA", lit(area))
        df = df.withColumn("FTD_FECHAHORA_ALTA", lit(current_timestamp()))

        _write_spark_dataframe(df, configure_postgres_spark, '"GESTOR"."TCGESPRO_MUESTRA"')
        eliminar_tablas()

        ###########################  IMAGENES   #################################################
        truncate_table(postgres_oci, 'TTEDOCTA_IMAGEN')

        delete_all_objects(bucket_name, prefix)

        delete_all_objects(bucket_name, 'profuturo-archivos')

        query = """
                SELECT
                DISTINCT
                concat("FTC_CODIGO_POSICION_PDF",'-',tcie."FCN_ID_FORMATO_ESTADO_CUENTA",'-',tcie."FCN_ID_AREA",'-',coalesce("FTC_RANGO_EDAD", ''), '-', COALESCE(tcie."FTC_DESCRIPCION_SIEFORE",'sinsiefore') ) AS ID,"FTO_IMAGEN" AS FTO_IMAGEN
                FROM
                "GESTOR"."TTGESPRO_CONFIG_IMAGEN_EDOCTA" tcie
                """

        try:
            imagenes_df = _create_spark_dataframe(spark, configure_postgres_spark, query,
                                                  params={"term": term_id, "start": start_month, "end": end_month,
                                                          "user": str(user)})
            imagenes = imagenes_df.collect()
            limit = 10_000
            if len(imagenes) < limit:
                for row in imagenes:
                    upload_to_gcs(row)
            else:
                print("La cantidad de imagenes a procesar supera el limite")

                # Obtiene la información del blob
            blob_info_list = get_blob_info(bucket_name, prefix)

            schema = StructType([
                StructField("FTC_POSICION_PDF", StringType(), True),
                StructField("FCN_ID_FORMATO_EDOCTA", IntegerType(), True),
                StructField("FCN_ID_AREA", IntegerType(), True),
                StructField("FTC_URL_IMAGEN", StringType(), True),
                StructField("FTC_IMAGEN", StringType(), True),
                StructField("FTC_SIEFORE", StringType(), True),
                StructField("FTC_RANGO_EDAD", StringType(), True)
            ])

            df = spark.createDataFrame(blob_info_list, schema=schema)

            _write_spark_dataframe(df, configure_postgres_oci_spark, '"ESTADO_CUENTA"."TTEDOCTA_IMAGEN"')

        except Exception as e:
            print(f"Error processing images: {str(e)}")

        ####################### FIN MUESTRAS



        for i in range(1000):
            headers = get_headers()  # Get the headers using the get_headers() function
            response = requests.get(url, headers=headers)  # Pass headers with the request
            print(response)

            if response.status_code == 200:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(20)
            else:
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        time.sleep(40)

        notify(
            postgres,
            "Generacion muestras retiros",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar las muestras de retiros de los estados de cuenta con éxito: por favor espere unos minutos",
            aprobar=False,
            descarga=False,
        )