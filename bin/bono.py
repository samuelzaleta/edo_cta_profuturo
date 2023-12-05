from profuturo.common import truncate_table, register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_mit_pool, configure_postgres_spark, configure_mit_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col, lit
from warnings import filterwarnings
import sys
from datetime import datetime

filterwarnings(action='ignore', category=DeprecationWarning, message='`np.bool` is a deprecated alias')
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
user = int(sys.argv[3])
print(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TTCALCUL_BONO', term=term_id)
        #truncate_table(postgres, 'TTHECHOS_RETIRO_LIQUIDACIONES', term=term_id)
        #truncate_table(postgres, 'TTHECHOS_RETIRO_SALDOS_INICIALES', term=term_id)


        query_dias_rend_bono = """
        SELECT RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR AS FTD_FECHA_REDENCION_BONO,
        CAST(EXTRACT(DAY FROM RB.FTD_FEH_VALOR - TRUNC(:end)) AS INTEGER) AS DIAS_PLAZO_NATURALES
        FROM TTAFOGRAL_OP_INVDUAL RB
        WHERE (RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR) IN (
                                SELECT FTN_NUM_CTA_INVDUAL,
                                       MAX(FTD_FEH_VALOR)
                                FROM TTAFOGRAL_OP_INVDUAL
                                GROUP BY FTN_NUM_CTA_INVDUAL)
        """

        query_vector = """
        SELECT FTN_PLAZO, FTN_FACTOR FROM TTAFOGRAL_VECTOR
        """

        query_saldos = """
        SELECT "FCN_CUENTA", "FTF_DIA_ACCIONES", "FTF_SALDO_DIA"
        FROM "HECHOS"."THHECHOS_SALDO_HISTORICO"
        WHERE "FCN_ID_PERIODO" = :term
        and "FCN_ID_TIPO_SUBCTA"  = 14
        """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_dias_rend_bono,
            "DIAS_REDENCION",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_vector,
            "VECTOR",
            params={"end": end_month}
        )


        read_table_insert_temp_view(
            configure_postgres_spark,
            query_saldos,
            "SALDOS",
            params={"term": term_id}
        )

        df = spark.sql("""
            SELECT 
            S.FCN_CUENTA,
            ROUND(S.FTF_DIA_ACCIONES,6) AS FTF_BON_NOM_ACC, 
            ROUND(S.FTF_SALDO_DIA,2) AS FTF_BON_NOM_PES,
            COALESCE(CASE 
            WHEN S.FTF_DIA_ACCIONES > 0 THEN ROUND(S.FTF_DIA_ACCIONES * X.FTN_FACTOR,6) END, 0) FTF_BON_ACT_ACC,
            COALESCE(CASE 
            WHEN S.FTF_SALDO_DIA > 0 THEN ROUND(S.FTF_SALDO_DIA * X.FTN_FACTOR,2) END, 0) FTF_BON_ACT_PES
            FROM (SELECT 
                    DR.FTN_NUM_CTA_INVDUAL,DR.FTD_FECHA_REDENCION_BONO,
                    DR.DIAS_PLAZO_NATURALES, VT.FTN_FACTOR
                    FROM DIAS_REDENCION DR
                        INNER JOIN VECTOR VT
                        ON VT.FTN_PLAZO = DR.DIAS_PLAZO_NATURALES
                ) X
                    INNER JOIN SALDOS S 
                    ON X.FTN_NUM_CTA_INVDUAL = S.FCN_CUENTA
        """)

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))

        _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TTCALCUL_BONO"')

