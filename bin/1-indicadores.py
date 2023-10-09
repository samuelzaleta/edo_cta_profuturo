from profuturo.common import register_time, define_extraction, truncate_table
from profuturo.database import get_postgres_pool, get_mit_pool, get_buc_pool
from profuturo.extraction import extract_indicator, update_indicator
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from sqlalchemy import text, Engine
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
buc_pool = get_buc_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción
        truncate_table(postgres, "TCHECHOS_CLIENTE", term=term_id)

        postgres.execute(text("""
        INSERT INTO "TCHECHOS_CLIENTE" ("FCN_CUENTA", "FCN_ID_PERIODO", "FTO_INDICADORES")
        SELECT "FTN_CUENTA", :term, '{}'::JSONB
        FROM "TCDATMAE_CLIENTE"
        """), {"term": term_id})

        # Fixed indicators
        update_indicator(mit, postgres, """
        SELECT IND.FTN_NUM_CTA_INVDUAL,
               CASE IND.FCC_VALOR_IND
                   WHEN '1' THEN 1
                   WHEN '0' THEN 0
               END AS valor_indicador
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 708
        """, """
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTB_PENSION" = (:value)::bool
        WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
        """, term=term_id)
        update_indicator(mit, postgres, """
        SELECT IND.FTN_NUM_CTA_INVDUAL,
               CASE IND.FCC_VALOR_IND
                   WHEN '66' THEN 'IMSS'
                   WHEN '67' THEN 'ISSSTE'
                   WHEN '68' THEN 'INDEPENDIENTE'
                   WHEN '69' THEN 'MIXTO'
               END AS valor_indicador
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 57
        """, """
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_TIPO_CLIENTE" = :value
        WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
        """, term=term_id)
        update_indicator(mit, postgres, """
        SELECT IND.FTN_NUM_CTA_INVDUAL,
               CASE IND.FCC_VALOR_IND
                   WHEN '713' THEN 'Asignado'
                   WHEN '714' THEN 'Afiliado'
               END AS valor_indicador
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 709
        """, """
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_ORIGEN" = :value
        WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
        """, term=term_id)
        update_indicator(mit, postgres, """
        SELECT IND.FTN_NUM_CTA_INVDUAL,
               CASE IND.FCC_VALOR_IND 
                   WHEN '1' THEN 'V'
                   WHEN '0' THEN 'N'
               END AS valor_indicador
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 58
        """, """
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_VIGENCIA" = :value
        WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
        """, term=term_id)
        update_indicator(mit, postgres, """
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT SH.FTN_NUM_CTA_INVDUAL, 'AFORE' AS value
        FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR LIKE '%SAR%' OR TS.FCC_VALOR LIKE '%92%'
        """, """
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_GENERACION" = :value
        WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
        """, term=term_id)
        update_indicator(mit, postgres, """
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT SH.FTN_NUM_CTA_INVDUAL, 'Transición' AS value
        FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR NOT LIKE '%SAR%' OR TS.FCC_VALOR NOT LIKE '%92%'
        """, """
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_GENERACION" = :value
        WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
        """, term=term_id)

        # Dynamic indicators
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
