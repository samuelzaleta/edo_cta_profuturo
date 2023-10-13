from sqlalchemy import text
from profuturo.common import register_time, define_extraction, truncate_table, notify
from profuturo.database import SparkConnectionConfigurator, get_postgres_pool, configure_mit_spark, \
                               configure_postgres_spark, configure_buc_spark, configure_integrity_spark, \
                               configure_bigquery_spark
from profuturo.extraction import update_indicator_spark, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from datetime import datetime
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        # Indicadores dinámicos
        truncate_table(postgres, "TCHECHOS_CLIENTE_INDICADOR", term=term_id)
        indicators = postgres.execute(text("""
        SELECT "FTN_ID_INDICADOR", "FTC_DESCRIPCION", "FTB_DISPONIBLE", "FTB_IMPRESION", "FTB_ENVIO", "FTB_GENERACION"
        FROM "TCGESPRO_INDICADOR"
        WHERE "FTB_ESTATUS" = true
        """))

        for indicator in indicators.fetchall():
            print(f"Extracting {indicator[1]}...")

            indicators_queries = postgres.execute(text("""
            SELECT "FTC_CONSULTA_SQL", "FTC_BD_ORIGEN"
            FROM "TCGESPRO_INDICADOR_CONSULTA"
            WHERE "FCN_ID_INDICADOR" = :indicator
            """), {"indicator": indicator[0]})

            for indicator_query in indicators_queries.fetchall():
                query = indicator_query[0]
                origin = indicator_query[1]

                origin_configurator: SparkConnectionConfigurator
                if origin == "BUC":
                    origin_configurator = configure_buc_spark
                elif origin == "MIT":
                    origin_configurator = configure_mit_spark
                elif origin == "INTGY":
                    origin_configurator = configure_integrity_spark('cierren')
                else:
                    origin_configurator = configure_postgres_spark

                update_indicator_spark(origin_configurator, configure_postgres_spark, query, indicator._mapping, term=term_id)

            print(f"Done extracting {indicator[1]}!")

        # Indicadores manuales
        extract_dataset_spark(configure_postgres_spark, configure_postgres_spark, """
        SELECT "FCN_CUENTA", "FTN_ID_INDICADOR" AS "FCN_ID_INDICADOR", 1 AS "FTN_EVALUA_INDICADOR"
        FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR" I ON CA."FTC_TRAMITE" = I."FTC_TRAMITE"
        WHERE "FCN_ID_PERIODO" = :term
        """, '"HECHOS"."TCHECHOS_CLIENTE_INDICADOR"', term=term_id, params={'term': term_id})

        extract_dataset_spark(configure_postgres_spark, configure_bigquery_spark, """
        SELECT "FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_INDICADOR", "FTN_EVALUA_INDICADOR", "FTA_EVALUA_INDICADOR"
        FROM "HECHOS"."TCHECHOS_CLIENTE_INDICADOR"
        """, "ESTADO_CUENTA.TEST_INDICADOR")

        notify(
            postgres,
            f"Indicadores ingestados",
            phase,
            area,
            term=term_id,
            message=f"Se han ingestado los indicadores de forma exitosa para el periodo {time_period}",
            validated=True,
            aprobar=False,
            descarga=False
        )