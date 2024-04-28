from profuturo.common import define_extraction, register_time, truncate_table, notify
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_postgres_spark, configure_postgres_oci_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms,  _get_spark_session, read_table_insert_temp_view
from pyspark.sql.functions import udf, concat, col, current_date , row_number,lit, current_timestamp
from profuturo.env import load_env
from datetime import datetime, timedelta
import requests
import time
import json
import sys
import jwt
import os


load_env()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
url = os.getenv("URL_DEFINITIVO_RET")
print(url)

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
    except Exception as error:
        print("ERROR:", error)
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
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TTEDOCTA_RETIRO_GENERAL')
        truncate_table(postgres, 'TTEDOCTA_RETIRO')

        read_table_insert_temp_view(configure_postgres_oci_spark, """
        SELECT
        DISTINCT
        C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
        F."FTN_ARCHIVO" AS "FTC_ARCHIVO",
        :term AS "FCN_ID_PERIODO",
        concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE",
        C."FTC_CALLE" AS "FTC_CALLE_NUMERO",
        C."FTC_COLONIA",
        C."FTC_DELEGACION" AS "FTC_MUNICIPIO",
        Cast(C."FTC_CODIGO_POSTAL" as varchar ) AS "FTC_CP",
        C."FTC_ENTIDAD_FEDERATIVA" AS "FTC_ENTIDAD",
        C."FTC_CURP",
        C."FTC_RFC",
        C."FTC_NSS"
        FROM "HECHOS"."TTHECHOS_RETIRO" F
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON F."FCN_CUENTA" = C."FTN_CUENTA"
         WHERE F."FCN_ID_PERIODO" = :term
        """, "edoCtaGenerales", params={"term": term_id, "user": str(user), "area": area})
        general_df = spark.sql("""
        select * from edoCtaGenerales
        """)

        general_df = general_df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            lit(term_id),
            col("FTC_ARCHIVO"),
        ).cast("bigint"))

        general_df = general_df.drop(col("FTC_ARCHIVO"))

        read_table_insert_temp_view(configure_postgres_oci_spark,
          """
                SELECT
                R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
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
                WHERE R."FCN_ID_PERIODO" = :term
                """, "edoCtaAnverso", params={"term": term_id, "user":str(user), "area": area})

        anverso_df = spark.sql("select * from edoCtaAnverso")

        anverso_df = anverso_df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            lit(term_id),
            col("FTC_ARCHIVO"),
        ).cast("bigint"))


        _write_spark_dataframe(general_df, configure_postgres_oci_spark, '"ESTADO_CUENTA"."TTEDOCTA_RETIRO_GENERAL"')
        _write_spark_dataframe(anverso_df, configure_postgres_oci_spark, '"ESTADO_CUENTA"."TTEDOCTA_RETIRO"')



        response = requests.get(url)

        headers = get_headers()  # Get the headers using the get_headers() function

        for i in range(1000):
            response = requests.get(url, headers=headers)  # Pass headers with the request
            print(response)

            if response.status_code == 200:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(8)
            else:
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        time.sleep(400)

        notify(
            postgres,
            "Generacion estados de cuenta retiros",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar las muestras de retiros de los estados de cuenta con éxito: por favor espere unos minutos",
            aprobar=False,
            descarga=False,
        )