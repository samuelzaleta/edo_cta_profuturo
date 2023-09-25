from profuturo.common import define_extraction, register_time
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, extract_dataset_spark, _get_spark_session, read_table_insert_temp_view
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf, concat, col, current_date
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

    read_table_insert_temp_view(configure_postgres_spark,
    f"""
       SELECT
       DISTINCT
       --M."FTC_URL_PDF_ORIGEN" as "FTC_URL_EDOCTA",
       F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",
       'CANDADO' AS "FTC_CANDADO_APERTURA",
       F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
       M."FCN_ID_PERIODO",
       REPLACE(PR."FTC_PERIODO", '/','') AS "PERIODO",
       C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
       -- as "FTB_PDF_IMPRESO",
       --current_date AS "FTD_FECHA_CORTE",
       --current_date AS "FTD_FECHA_GRAL_INICIO",
       --current_date AS "FTD_FECHA_GRAL_FIN",
       --current_date AS "FTD_FECHA_MOV_INICIO",
       --current_date AS "FTD_FECHA_MOV_FIN",
       0.0 AS "FTN_SALDO_SUBTOTAL",
       0.0 AS "FTN_SALDO_TOTAL",
       0 AS "FTN_ID_SIEFORE",
       Cast(I."FTC_PERFIL_INVERSION" as varchar) AS "FTC_DESC_SIEFORE",
       Cast(I."FTC_GENERACION" as varchar) AS "FTC_DESC_TIPOGENERACION",
       concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE_COMPLETO",
       C."FTC_CALLE" AS "FTC_CALLE_NUMERO",
       C."FTC_COLONIA",
       C."FTC_DELEGACION",
       C."FTN_CODIGO_POSTAL" AS "FTN_CP",
       C."FTC_ENTIDAD_FEDERATIVA",
       C."FTC_NSS",
       C."FTC_RFC",
       C."FTC_CURP",
       now() AS "FTD_FECHAHORA_ALTA",
       '0' AS "FTC_USUARIO_ALTA"
       FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
       INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P
       ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
       INNER JOIN "GESTOR"."TEMP_CONFIGURACION" CF
       ON F."FCN_ID_GENERACION" = CF."FCN_GENERACION"
       INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
       ON I."FCN_ID_PERIODO" = 1 --:term
       AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
       INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C
       ON I."FCN_CUENTA" = C."FTN_CUENTA"
       INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M
       ON C."FTN_CUENTA" = M."FCN_CUENTA"
       INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR
       ON PR."FTN_ID_PERIODO" = 1 --:term
   """, "edoCtaGenerales",
    params={"term": term_id, "user": str(user)}
    )

    uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
    df = spark.sql("""
                               select  * from edoCtaGenerales
                               """)

    df = df.withColumn("FCN_ID_EDOCTA", concat(
        col("FCN_NUMERO_CUENTA"),
        col("PERIODO"),
        col("FTN_ID_FORMATO"),
    ).cast("bigint"))
    df.select("FCN_ID_EDOCTA").show()

    df = df.withColumn("FCN_FOLIO", uuidUdf())
    df = df.drop(col("PERIODO"))

    _write_spark_dataframe(df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_GENERAL')

    df.createOrReplaceTempView("general")

    read_table_insert_temp_view(configure_postgres_spark,
    f"""
            SELECT
            "FTN_ID_MUESTRA",
            "FCN_CUENTA",
            "FCN_ID_PERIODO",
            "FTC_URL_PDF_ORIGEN",
            "FTC_ESTATUS",
            "FCN_ID_USUARIO",
            "FCN_ID_AREA",
            "FTD_FECHAHORA_ALTA"
            FROM "GESTOR"."TCGESPRO_MUESTRA"
          """, "muestras",
          params={"term": term_id, "user": str(user)}
        )
    df = spark.sql("""
    select
    "FTN_ID_MUESTRA",
    "FCN_CUENTA",
    "FCN_ID_PERIODO",
    Concat("https://storage.cloud.google.com/profuturo-archivos/",e.FCN_FOLIO) as "FTC_URL_PDF_ORIGEN",
    "FTC_ESTATUS",
    "FCN_ID_USUARIO",
    "FCN_ID_AREA",
    "FTD_FECHAHORA_ALTA"
    from muestras m
    right join general e
    on m.FCN_CUENTA = e.FCN_NUMERO_CUENTA  
    """)

    df.show(1)
