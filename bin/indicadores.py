
from profuturo.common import register_time, define_extraction, truncate_table
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import update_indicator_spark
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from sqlalchemy import text
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id):
        # Extracción
        truncate_table(postgres, 'TCHECHOS_CLIENTE', term=term_id)

        # Fixed indicators
        truncate_table(postgres, 'TCHECHOS_INDICADOR')
        update_indicator_spark(configure_mit_spark, configure_postgres_spark, """
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '1' THEN 1
                   WHEN '0' THEN 0
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 708
        """)
        postgres.execute(text("""
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTB_PENSION" = i."FCC_VALOR"::bool
        FROM "TCHECHOS_CLIENTE" AS c
        INNER JOIN "TCHECHOS_INDICADOR" AS i ON c."FCN_CUENTA" = i."FCN_CUENTA"
        """))

        truncate_table(postgres, 'TCHECHOS_INDICADOR')
        update_indicator_spark(configure_mit_spark, configure_postgres_spark, """
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '66' THEN 66 --'IMSS'
                   WHEN '67' THEN 67 --'ISSSTE'
                   WHEN '68' THEN 68 --'INDEPENDIENTE'
                   WHEN '69' THEN 69 --'MIXTO'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 57
        """)
        postgres.execute(text("""
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_TIPO_CLIENTE" = i."FCC_VALOR"::varchar
        FROM "TCHECHOS_CLIENTE" AS c
        INNER JOIN "TCHECHOS_INDICADOR" AS i ON c."FCN_CUENTA" = i."FCN_CUENTA"
        """))

        truncate_table(postgres, 'TCHECHOS_INDICADOR')
        update_indicator_spark(configure_mit_spark, configure_postgres_spark, """
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '713' THEN 713 --'Asignado'
                   WHEN '714' THEN 714 --'Afiliado'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 709
        """)
        postgres.execute(text("""
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_ORIGEN" = i."FCC_VALOR"::varchar
        FROM "TCHECHOS_CLIENTE" AS c
        INNER JOIN "TCHECHOS_INDICADOR" AS i ON c."FCN_CUENTA" = i."FCN_CUENTA"
        """))

        truncate_table(postgres, 'TCHECHOS_INDICADOR')
        update_indicator_spark(configure_mit_spark, configure_postgres_spark, """
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND 
                   WHEN '1' THEN 1 --'V'
                   WHEN '0' THEN 0 --'N'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE FCN_ID_INDICADOR = 58
        """)
        postgres.execute(text("""
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_VIGENCIA" = i."FCC_VALOR"::char
        FROM "TCHECHOS_CLIENTE" AS c
        INNER JOIN "TCHECHOS_INDICADOR" AS i ON c."FCN_CUENTA" = i."FCN_CUENTA"
        """))

        truncate_table(postgres, 'TCHECHOS_INDICADOR')
        update_indicator_spark(configure_mit_spark, configure_postgres_spark, """
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               2 /* AFORE */ AS FCC_VALOR
        FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR LIKE '%SAR%' OR TS.FCC_VALOR LIKE '%92%'
        """)
        postgres.execute(text("""
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTC_GENERACION" = i."FCC_VALOR"::varchar
        FROM "TCHECHOS_CLIENTE" AS c
        INNER JOIN "TCHECHOS_INDICADOR" AS i ON c."FCN_CUENTA" = i."FCN_CUENTA"
        """))

        truncate_table(postgres, 'TCHECHOS_INDICADOR')
        update_indicator_spark(configure_mit_spark, configure_postgres_spark, """
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               3 /* TRANSICION */ AS FCC_VALOR
        FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR NOT LIKE '%SAR%' OR TS.FCC_VALOR NOT LIKE '%92%'
        """)
        postgres.execute(text("""
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTB_BONO" = i."FCC_VALOR"::bool
        FROM "TCHECHOS_CLIENTE" AS c
        INNER JOIN "TCHECHOS_INDICADOR" AS i ON c."FCN_CUENTA" = i."FCN_CUENTA"
        """))
