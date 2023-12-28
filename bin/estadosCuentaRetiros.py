from profuturo.common import define_extraction, register_time, truncate_table, notify
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool
from profuturo.extraction import _write_spark_dataframe, extract_terms,  _get_spark_session, read_table_insert_temp_view
from pyspark.sql.functions import udf, concat, col, current_date , row_number,lit, current_timestamp
import sys
import requests
import time



#url = "https://procesos-api-service-dev-e46ynxyutq-uk.a.run.app/procesos/generarEstadosCuentaRetiros/muestras"
url = "https://procesos-api-service-qa-2ky75pylsa-uk.a.run.app/procesos/generarEstadosCuentaRecaudaciones/definitivos"
postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool,bigquery_pool) as (postgres, bigquery):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TCGESPRO_MUESTRA', term=term_id, area=area)
        truncate_table(bigquery, 'ESTADO_CUENTA.TTEDOCTA_RETIRO_GENERAL')
        truncate_table(bigquery, 'ESTADO_CUENTA.TTEDOCTA_RETIRO')

        read_table_insert_temp_view(configure_postgres_spark, """
        SELECT
        DISTINCT
        C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
        F."FTN_ARCHIVO" AS "FTC_ARCHIVO",
        :term AS "FCN_ID_PERIODO",
        concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE",
        C."FTC_CALLE" AS "FTC_CALLE_NUMERO",
        C."FTC_COLONIA",
        C."FTC_DELEGACION" AS "FTC_MUNICIPIO",
        Cast(C."FTN_CODIGO_POSTAL" as varchar ) AS "FTC_CP",
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

        read_table_insert_temp_view(configure_postgres_spark,
          """
                SELECT
                R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
                --"FTC_FOLIO",
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
                WHEN "FTC_FON_ENTIDAD" is not null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET"
                ELSE 0
                END "FTN_FON_MONTO_TRANSF",
                0.0 AS "TFN_FON_RETENCION_ISR",
                "FTD_FECHA_EMISION" AS "FTD_FECHA_EMISION",
                "FTC_ENT_REC_TRAN" AS "FTC_AFO_ENTIDAD",
                "FCC_MEDIO_PAGO" AS "FTC_AFO_MEDIO_PAGO",
                CASE
                WHEN "FTC_FON_ENTIDAD" is null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET" - "FTN_AFO_ISR"
                ELSE 0
                END "FTC_AFO_RECURSOS_ENTREGA",
                "FTD_FECHA_EMISION" AS "FTD_AFO_FECHA_ENTREGA",
                "FTN_AFO_ISR" AS "FTC_AFO_RETENCION_ISR",
                CAST("FTN_FEH_INI_PEN" AS INT) AS "FTC_FECHA_INICIO_PENSION",
                CAST("FTN_FEH_RES_PEN" as INT) AS "FTC_FECHA_EMISION",
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


        _write_spark_dataframe(general_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTEDOCTA_RETIRO_GENERAL')
        _write_spark_dataframe(anverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTEDOCTA_RETIRO')

        response = requests.get(url)


        # Verifica si la petición fue exitosa
        if response.status_code == 200:
            # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
            content = response.content
            print(content)
        else:
            # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
            print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")

        time.sleep(400)

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