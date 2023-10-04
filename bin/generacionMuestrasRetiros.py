from profuturo.common import define_extraction, register_time, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, extract_dataset_spark, _get_spark_session, read_table_insert_temp_view
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf, concat, col, current_date , row_number,lit, current_timestamp
from pyspark.sql.window import Window
import uuid
import sys

postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase=phase, area=area, usuario=user, term=term_id):
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
        FROM "HECHOS"."TTHECHOS_RETIRO_TEST" F
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON F."FCN_CUENTA" = C."FTN_CUENTA"
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
                "FTF_SALDO_INICIAL" AS "FTN_SDO_INI_AHO_RET",
                "FTF_SALDO_INICIAL" AS "FTN_SDO_INI_AHO_VIV",
                "FTF_MONTO_PESOS_LIQUIDADO" "FTN_SDO_TRA_AHO_RET",
                "FTF_MONTO_PESOS_LIQUIDADO" "FTN_SDO_TRA_AHO_VIV",
                ("FTF_SALDO_INICIAL" - "FTF_MONTO_PESOS_LIQUIDADO") AS "FTN_SDO_REM_AHO_RET",
                ("FTF_SALDO_INICIAL" - "FTF_MONTO_PESOS_LIQUIDADO") AS "FTN_SDO_REM_AHO_VIV",
                --"FTC_TIPO_TRAMITE",
                "FTC_LEY_PENSION" AS "FTC_LEY_PENSION",
                --"FTC_TIPO_PRESTACION",
                "FTC_CVE_REGIMEN" AS "FTC_REGIMEN",
                "FTC_CVE_TIPO_SEG" AS "FTC_SEGURO",
                "FTC_CVE_TIPO_PEN" AS "FTC_TIPO_PENSION",
                --"FTC_TPSEGURO" AS FTC_SEGURO,
                --"FTC_TPPENSION" AS FTC_TIPO_PENSION,
                --"FTC_REGIMEN" AS FTC_REGIMEN,
                CASE "FTC_LEY_PENSION"
                WHEN 'IMSS' THEN 'ASEGURADORA'
                ELSE 'GOBIERNO FEDERAL'
                END "FTC_FON_ENTIDAD",
                NULL AS "FTC_FON_NUMERO_POLIZA",
                "FTF_MONTO_PESOS_LIQUIDADO" AS "FTN_FON_MONTO_TRANSF",
                --DATE '2023-03-01' AS "FTD_FON_FECHA_TRANSF",
                "FTF_ISR" AS "TFN_FON_RETENCION_ISR",
                "FTC_TIPO_BANCO" AS "FTC_AFO_ENTIDAD",
                "FTC_MEDIO_PAGO" AS "FTC_AFO_MEDIO_PAGO",
                "FTF_MONTO_PESOS_LIQUIDADO" AS "FTC_AFO_RECURSOS_ENTREGA",
                --NULL AS "FTD_AFO_FECHA_ENTREGA",
                "FTF_ISR" AS "FTC_AFO_RETENCION_ISR",
                --DATE '2023-03-01' AS "FTD_FECHA_EMISION",
                --DATE '2023-03-01' AS "FTD_FECHA_INICIO_PENSION",
                "FTC_TMC_DESC_ITGY" AS "FTN_TIPO_TRAMITE",
                ("FTF_SALDO_INICIAL" - "FTF_MONTO_PESOS_LIQUIDADO")  AS "FTN_SALDO_FINAL",
                CASE "FTC_CVE_REGIMEN"
                WHEN 'RO' THEN '73'
                WHEN '73' THEN '73'
                ELSE '93'
                END "FTC_ARCHIVO"
                FROM (
                SELECT
                row_number() over (PARTITION BY "FCN_CUENTA" order by "FTN_TIPO_AHORRO") as rowid,
                *
                FROM "HECHOS"."TTHECHOS_RETIRO_TEST"
                WHERE "FTN_TIPO_AHORRO" = 1
                ) x where rowid =1
                """, "edoCtaAnverso", params={"term": term_id, "start": start_month,"end": end_month, "user": str(user)})
        anverso_df = spark.sql("select * from edoCtaAnverso")


        #_write_spark_dataframe(general_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_RETIRO_GENERAL')
        _write_spark_dataframe(anverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_RETIRO')
