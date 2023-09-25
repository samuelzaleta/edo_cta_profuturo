from profuturo.common import define_extraction, register_time
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, extract_dataset_spark, _get_spark_session, read_table_insert_temp_view
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf
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
    """
    SELECT
        M."FTC_URL_PDF_ORIGEN",
        F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",
        'CANDADO' AS "FTC_CANDADO_APERTURA",
        F."FCN_ID_FORMATO_ESTADO_CUENTA" AS FTN_ID_FORMATO,
        M."FCN_ID_PERIODO",
        C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
        now() AS "FTD_FECHA_CORTE",
        now() AS "FTD_FECHA_GRAL_INICIO",
        now() AS "FTD_FECHA_GRAL_FIN",
        now() AS "FTD_FECHA_MOV_INICIO",
        now() AS "FTD_FECHA_MOV_FIN",
        0 AS "FTF_SALDO_SUBTOTAL",
        0 AS "FTF_SALDO_TOTAL",
        0 AS "FTN_ID_SIEFORE",
        I."FTC_PERFIL_INVERSION" AS "FTN_DESC_SIEFORE",
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
        :user AS "FTC_USUARIO_ALTA"
    FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
    INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P
    ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
    INNER JOIN "GESTOR"."TEMP_CONFIGURACION" CF
    ON F."FCN_ID_GENERACION" = CF."FCN_GENERACION"
    INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
    ON I."FCN_ID_PERIODO" = :term AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
    INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C
    ON I."FCN_CUENTA" = C."FTN_CUENTA"
    --INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M
    --ON C."FTN_CUENTA" = M."FCN_CUENTA"
    """,
    "edoCtaGenerales",
    {'user': user}
    )

    uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
    df = spark.sql("""
    select  * from edoCtaGenerales
    """)
    df = df.withColumn("FCN_ID_EDOCTA",uuidUdf())
    df = df.withColumn("FCN_FOLIO", uuidUdf())
    _write_spark_dataframe(df, configure_bigquery_spark, '"ESTADO_CUENTA"."TEST_TTEDOCTA_REVERSO"')

    with register_time(postgres_pool, phase=phase, area=area, usuario=user, term=term_id):
        extract_dataset_spark(
            configure_postgres_spark,
            configure_bigquery_spark,
            """
            select F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FCN_ID_EDOCTA",
                   MC."FTC_MOV_TIPO_AHORRO" AS "FTC_SECCION",
                   R."FCN_CUENTA",
                   R."FTD_FEH_LIQUIDACION" as "FTD_FECHA_MOVIMIENTO",
                   MC."FTN_ID_MOVIMIENTO_CONSAR" AS "FTN_ID_CONCEPTO",
                   MC."FTC_DESCRIPCION" AS "FTC_DESC_CONCEPTO",
                   'Desconocido' AS "FTN_PERIODO_REFERENCIA",
                   sum(R."FTF_MONTO_PESOS") as "FTF_MONTO",
                   now() AS "FTD_FECHAHORA_ALTA",
                   :user AS "FTC_USUARIO_ALTA"
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TEMP_CONFIGURACION" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON I."FCN_ID_PERIODO" = :term AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                INNER JOIN "HECHOS"."TTHECHOS_MOVIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
                INNER JOIN "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FTN_ID_MOVIMIENTO_CONSAR"
                --INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
                --INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FTN_ID_MOVIMIENTO_PROFUTURO"
                --INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA" AND I."FCN_ID_PERIODO" = R."FCN_ID_PERIODO"
                WHERE mod(extract(MONTH FROM to_date(T."FTC_PERIODO", 'MM/YYYY')), P."FTN_MESES") = 0
                  AND to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * P."FTN_MESES" AND :end
                GROUP BY F."FCN_ID_FORMATO_ESTADO_CUENTA", R."FCN_CUENTA", MC."FTN_ID_MOVIMIENTO_CONSAR", R."FTD_FEH_LIQUIDACION"
            """,
            "ESTADO_CUENTA.TEST_TTEDOCTA_REVERSO",
            params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)},
        )