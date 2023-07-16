from profuturo.common import notify, register_time, define_extraction, truncate_table
from profuturo.database import get_postgres_pool, get_mit_pool, get_buc_pool
from profuturo.extraction import extract_indicator
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from sqlalchemy import text, Engine
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
buc_pool = get_buc_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, buc_pool) as (postgres, buc):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id):
        # Extracción
        truncate_table(postgres, "TCHECHOS_CLIENTE", term=term_id)

        postgres.execute("""
        INSERT INTO "TCHECHOS_CLIENTE" ("FCN_CUENTA", "FCN_ID_PERIODO", "FTO_INDICADORES")
        SELECT "FTN_CUENTA", :term, '{}'::JSONB
        FROM "TCDATMAE_CLIENTE"
        """, {"term": term_id})

        indicators = postgres.execute(text("""
        SELECT "FTN_ID_INDICADOR", "FTC_DESCRIPCION" 
        FROM "TCGESPRO_INDICADOR"
        WHERE "FTC_BANDERA_ESTATUS" = 'V'
        """))

        for indicator in indicators.fetchall():
            print(f"Extracting {indicator[1]}...")

            indicators_queries = postgres.execute(text("""
            SELECT "FTC_CONSULTA_SQL", "FTC_BD_ORIGEN"
            FROM "TCGESPRO_INDICADOR_CONSULTA"
            WHERE "FCN_ID_INDICADOR" = :indicator
            """), {"indicator": indicator[0]})

            for indicator_query in indicators_queries.fetchall():
                pool: Engine
                if indicator_query[1] == "BUC":
                    pool = buc_pool
                elif indicator_query[1] == "MIT":
                    pool = mit_pool
                else:
                    pool = postgres_pool

                with pool.connect() as conn:
                    extract_indicator(conn, postgres, indicator_query[0], indicator[0], term_id)

            print(f"Done extracting {indicator[1]}!")

        # Generación de cifras de control
        report = html_reporter.generate(
            postgres,
            """
            SELECT "FTO_INDICADORES"->>'34' AS generacion,
                  "FTO_INDICADORES"->>'21' AS vigencia,
                  CASE
                      WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                      WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                      WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                  END AS tipo_afiliación,
                  "FTO_INDICADORES"->>'31' AS tipo_cliente,
                  COUNT(*)
            FROM "TCDATMAE_CLIENTE" c
                INNER JOIN "TCHECHOS_CLIENTE" i ON c."FTN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            GROUP BY "FTO_INDICADORES"->>'34',
                    "FTO_INDICADORES"->>'21',
                    CASE
                        WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                        WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                        WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                    END,
                    "FTO_INDICADORES"->>'31'
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación"],
            ["Clientes"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control Cliente generadas",
            "Se han generado las cifras de control para clientes exitosamente",
            report,
            term=term_id,
            control=True,
        )
