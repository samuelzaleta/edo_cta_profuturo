from profuturo.common import register_time, define_extraction, truncate_table, notify
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    read_table_insert_temp_view(configure_mit_spark, """
            SELECT * FROM (SELECT FCN_CUENTA, FTF_MONTO_PESOS FROM (SELECT
             FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
             FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
             FTN_ID_MOV AS FCN_ID_MOVIMIENTO,
             FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
             FCN_ID_SIEFORE,
             FTC_FOLIO,
             FTF_MONTO_ACCIONES,
             FTD_FEH_LIQUIDACION,
             FTF_MONTO_PESOS
             FROM CIERREN.TTAFOGRAL_MOV_CMS
             WHERE FTD_FEH_LIQUIDACION BETWEEN to_date('01/01/2023','dd/MM/yyyy')
                 AND to_date('31/01/2023','dd/MM/yyyy'))
             ORDER BY FTF_MONTO_PESOS DESC)
             --WHERE ROWNUM <=100000
            """, "comisionesMIT")
    read_table_insert_temp_view(configure_postgres_spark, """
                select  "FCN_CUENTA", "FTF_MONTO_PESOS", "FCN_ID_SIEFORE", "FTD_FEH_LIQUIDACION"  FROM "HECHOS"."TTHECHOS_COMISION" m
                where "FCN_ID_PERIODO" = 27
                ORDER BY "FTF_MONTO_PESOS" DESC
                """, "comisionesPostgres")
    spark.sql("Select count(1) as comisionesPostgres  from comisionesPostgres").show()
    spark.sql("Select count(1) as comisionesMIT  from comisionesMIT").show()
    spark.sql("Select sum(FTF_MONTO_PESOS) as sumComisionesPostgres  from comisionesPostgres").show()
    spark.sql("Select sum(FTF_MONTO_PESOS)  as sumComisionesMIT  from comisionesMIT").show()

    df = spark.sql("""
    Select 
    *,
    IF(cp.FTF_MONTO_PESOS = cm.FTF_MONTO_PESOS, 1, 0 ) AS COMPARACION
    FROM comisionesPostgres cp
    INNER JOIN comisionesMIT cm ON cp.FCN_CUENTA = cm.FCN_CUENTA 
    and cp.FCN_ID_SIEFORE = cm.FCN_ID_SIEFORE
    and cp.FTD_FEH_LIQUIDACION = cm.FTD_FEH_LIQUIDACION
    WHERE IF(cp.FCN_CUENTA = cm.FCN_CUENTA, 1, 0 ) = 1
    """)
    print(df.count())
    df.show(5)
    df1= spark.sql("""
        SELECT * FROM (SELECT 
        *,
        IF(cp.FCN_CUENTA = cm.FCN_CUENTA, 1, 0 ) AS COMPARACION
        FROM comisionesPostgres cp
        INNER JOIN comisionesMIT cm ON cp.FCN_CUENTA = cm.FCN_CUENTA
        ) WHERE COMPARACION = 0
        """)
    print(df1.count())