from profuturo.common import define_extraction, register_time, truncate_table, notify
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, extract_dataset_spark, _get_spark_session, read_table_insert_temp_view
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf, concat, col, current_date , row_number,lit, current_timestamp
from pyspark.sql.window import Window
import uuid
import sys
import requests


url = "http://104.155.190.173:8080/GeneraRetirosPdf"

postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TCGESPRO_MUESTRA', term=term_id, area=area)
        read_table_insert_temp_view(configure_postgres_spark, """
        SELECT
        DISTINCT
        C."FTN_CUENTA" as "FCN_ID_EDOCTA",
        C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
        202303 as "FCN_ID_PERIODO",
        concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE",
        C."FTC_CALLE" AS "FTC_CALLE_NUMERO",
        C."FTC_COLONIA",
        C."FTC_DELEGACION" AS "FTC_MUNICIPIO",
        Cast(C."FTN_CODIGO_POSTAL" as varchar ) AS "FTC_CP",
        C."FTC_ENTIDAD_FEDERATIVA" AS "FTC_ENTIDAD",
        C."FTC_CURP",
        C."FTC_RFC",
        C."FTC_NSS",
        now() AS "FECHAHORA_ALTA",
        '0' AS "USUARIO"
        FROM "HECHOS"."TTHECHOS_RETIRO" F
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON F."FCN_CUENTA" = C."FTN_CUENTA"
        INNER JOIN "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA ON CA."FCN_CUENTA" = C."FTN_CUENTA" AND CA."FCN_ID_INDICADOR" = 32
        where C."FTN_CUENTA" is not null
        """, "edoCtaGenerales", params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        general_df = spark.sql("""
        select * from edoCtaGenerales
        """)

        read_table_insert_temp_view(configure_postgres_spark,
          """
              SELECT
                DISTINCT
                cast("FCN_CUENTA" as BIGINT) AS "FCN_ID_EDOCTA",
                cast("FCN_CUENTA" as BIGINT)  AS "FCN_NUMERO_CUENTA",
                202303 AS "FCN_ID_PERIODO",
                "FTF_SALDO_INI" AS "FTN_SDO_INI_AHO_RET",
                "FTF_SALDO_INI" AS "FTN_SDO_INI_AHO_VIV",
                "FTF_MONTO_LIQ" "FTN_SDO_TRA_AHO_RET",
                "FTF_MONTO_LIQ" "FTN_SDO_TRA_AHO_VIV",
                ("FTF_SALDO_INI" - "FTF_MONTO_LIQ") AS "FTN_SDO_REM_AHO_RET",
                ("FTF_SALDO_INI" - "FTF_MONTO_LIQ") AS "FTN_SDO_REM_AHO_VIV",
                --"FTC_TIPO_TRAMITE",
                "FTC_LEY_PENSION" AS "FTC_LEY_PENSION",
                --"FTC_TIPO_PRESTACION",
                "FTC_REGIMEN" AS "FTC_REGIMEN",
                "FTC_TPSEGURO" AS "FTC_SEGURO",
                "FTC_TPPENSION" AS "FTC_TIPO_PENSION",
                --"FTC_TPSEGURO" AS FTC_SEGURO,
                --"FTC_TPPENSION" AS FTC_TIPO_PENSION,
                --"FTC_REGIMEN" AS FTC_REGIMEN,
                "FTC_FON_ENTIDAD" AS "FTC_FON_ENTIDAD",
                NULL AS "FTC_FON_NUMERO_POLIZA",
                "FTF_MONTO_LIQ" AS "FTN_FON_MONTO_TRANSF",
                --DATE '2023-03-01' AS "FTD_FON_FECHA_TRANSF",
                "FTN_ISR_LIQ" AS "TFN_FON_RETENCION_ISR",
                "FCC_TIPO_BANCO" AS "FTC_AFO_ENTIDAD",
                "FCC_MEDIO_PAGO" AS "FTC_AFO_MEDIO_PAGO",
                "FTF_MONTO_LIQ" AS "FTC_AFO_RECURSOS_ENTREGA",
                --NULL AS "FTD_AFO_FECHA_ENTREGA",
                "FTN_ISR_LIQ" AS "FTC_AFO_RETENCION_ISR",
                --DATE '2023-03-01' AS "FTD_FECHA_EMISION",
                --DATE '2023-03-01' AS "FTD_FECHA_INICIO_PENSION",
                "FTC_TMC_DESC_ITGY" AS "FTN_TIPO_TRAMITE",
                ("FTF_SALDO_INI" - "FTF_MONTO_LIQ")  AS "FTN_SALDO_FINAL",
                "ARCHIVO" AS "FTC_ARCHIVO"
                FROM (
                SELECT
                row_number() over (PARTITION BY R."FCN_CUENTA" order by "FTN_TIPO_AHORRO") as rowid,
                R.*
                FROM "HECHOS"."TTHECHOS_RETIRO" R
                INNER JOIN "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA ON CA."FCN_CUENTA" = R."FCN_CUENTA" AND CA."FCN_ID_INDICADOR" = 32
                WHERE "FTN_TIPO_AHORRO" = 0
                ) x where rowid =1
                """, "edoCtaAnverso", params={"term": term_id, "start": start_month,"end": end_month, "user": str(user)})
        anverso_df = spark.sql("select * from edoCtaAnverso")


        _write_spark_dataframe(general_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_RETIRO_GENERAL')
        _write_spark_dataframe(anverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_RETIRO')

        df = spark.sql("""
        SELECT 
         FCN_NUMERO_CUENTA AS FCN_CUENTA
        FROM edoCtaGenerales
        """)

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))
        df = df.withColumn("FCN_ID_USUARIO", lit(user))
        df = df.withColumn("FCN_ID_AREA", lit(area))
        df = df.withColumn("FTD_FECHAHORA_ALTA", lit(current_timestamp()))
        df = df.withColumn("FTC_URL_PDF_ORIGEN", concat(
            lit("https://storage.googleapis.com/profuturo-archivos/"),
            col("FCN_CUENTA"),
            lit(".pdf"),
        ))

        #_write_spark_dataframe(df, configure_postgres_spark, '"GESTOR"."TCGESPRO_MUESTRA"')

        response = requests.get(url)

        # Verifica si la petición fue exitosa
        if response.status_code == 200:
            # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
            content = response.content
            print(content)
        else:
            # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
            print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")

        notify(
            postgres,
            "Generacion muestras retiros",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar las muestras de retiros de los estados de cuenta con éxito",
            aprobar=False,
            descarga=False,
        )
