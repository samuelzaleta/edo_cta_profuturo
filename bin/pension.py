from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col, lit
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    print(term_id)
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        query_pension = """
                        SELECT
                        PG.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                        CASE
                        WHEN PG.FCN_ID_SUBPROCESO = 310 THEN 'PENSIÓN MÍNIMA GARANTIZADA ISSSTE'
                            ELSE 'PENSIÓN MÍNIMA GARANTIZADA IMSS'
                            END FTC_TIPO_PENSION,
                        SUM(PGS.FTN_MONTO_PESOS) AS FTN_MONTO_PEN
                        FROM BENEFICIOS.TTCRXGRAL_PAGO PG
                            INNER JOIN BENEFICIOS.TTCRXGRAL_PAGO_SUBCTA PGS
                            ON PGS.FTC_FOLIO = PG.FTC_FOLIO AND PGS.FTC_FOLIO_LIQUIDACION = PG.FTC_FOLIO_LIQUIDACION
                            INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE TR
                            ON TR.FTN_FOLIO_TRAMITE = PG.FTN_FOLIO_TRAMITE
                        WHERE PG.FCN_ID_PROCESO IN (4050,4051)
                                AND PG.FCN_ID_SUBPROCESO IN (309, 310)
                                AND PGS.FCN_ID_TIPO_SUBCTA NOT IN (15,16,17,18)
                                AND TRUNC(PG.FTD_FEH_LIQUIDACION) <= :end
                        GROUP BY
                        PG.FTN_NUM_CTA_INVDUAL,PG.FCN_ID_SUBPROCESO
                        """

        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query_pension,
            '"MAESTROS"."TCDATMAE_PENSION"',
            params={"end": end_month,},
        )


