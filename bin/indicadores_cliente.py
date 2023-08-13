from profuturo.common import register_time, define_extraction, truncate_table
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import update_indicator_spark, _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from sqlalchemy import text
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])

with (define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _)):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id):
        spark = _get_spark_session()

        # Extracción
        #truncate_table(postgres, 'TCHECHOS_CLIENTE', term=term_id)
        read_table_insert_temp_view(configure_mit_spark,"""
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '1' THEN 1
                   WHEN '0' THEN 0
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI IN (11)
        AND FTC_VIGENCIA= 1
        """, "indicador_pension", params={"date": start_month, "type": "I"})

        spark.sql("select count(*) as count_indicador_pension from indicador_pension").show()

        read_table_insert_temp_view(configure_mit_spark,"""
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '66' THEN 66 --'IMSS'
                   WHEN '67' THEN 67 --'ISSSTE'
                   WHEN '68' THEN 68 --'INDEPENDIENTE'
                   WHEN '69' THEN 69 --'MIXTO'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI IN (1)
        AND FTC_VIGENCIA= 1
        """, "indicador_origen", params={"date": start_month, "type": "I"})
        spark.sql("select count(*) as count_indicador_origen from indicador_origen").show()

        read_table_insert_temp_view(configure_mit_spark,"""
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '713' THEN 713 --'Asignado'
                   WHEN '714' THEN 714 --'Afiliado'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI IN (12)
        AND FTC_VIGENCIA= 1
        """, "indicador_tipo_cliente", params={"date": start_month, "type": "I"})
        spark.sql("select count(*) as count_indicador_tipo_cliente from indicador_tipo_cliente").show()

        read_table_insert_temp_view(configure_mit_spark,"""
        SELECT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND 
                   WHEN '1' THEN 1 --'V'
                   WHEN '0' THEN 0 --'N'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI IN (2)
        AND FTC_VIGENCIA= 1
        """, "indicador_vigencia", params={"date": start_month, "type": "I"})
        spark.sql("select count(*) as count_indicador_vigencia from indicador_vigencia").show()

        read_table_insert_temp_view(configure_mit_spark,"""
        SELECT DISTINCT FTN_NUM_CTA_INVDUAL, 1 
               FROM CIERREN.THAFOGRAL_SALDO_HISTORICO_V2
               WHERE FCN_ID_SIEFORE = 81
        """, "indicador_bono", params={"date": start_month, "type": "I"})
        spark.sql("select count(*) as count_indicador_bono from indicador_bono").show()

        read_table_insert_temp_view(configure_mit_spark,"""
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               2 /* AFORE */ AS FCC_VALOR
        FROM cierren.thafogral_saldo_historico_v2 SH
        INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR LIKE '%SAR%' OR TS.FCC_VALOR LIKE '%92%'        
        """, "generacion_afore", params={"date": start_month, "type": "I"})
        spark.sql("select count(*) as count_generacion_afore from generacion_afore").show()

        read_table_insert_temp_view(configure_mit_spark,"""
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               3 /* TRANSICION */ AS FCC_VALOR
        FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR NOT LIKE '%SAR%' OR TS.FCC_VALOR NOT LIKE '%92%'
        """, "generacion_transicion", params={"date": start_month, "type": "I"})
        spark.sql("select count(*) as count_generacion_transicion from generacion_transicion").show()

        df =spark.sql(f"""
        With database_generacion as (
        SELECT FCN_CUENTA, FCC_VALOR from generacion_afore ga
        UNION ALL
        SELECT FCN_CUENTA, FCC_VALOR FROM generacion_transicion gt
        UNION ALL
        SELECT FCN_CUENTA, 4 AS VALOR FROM indicador_origen ori
        WHERE FCC_VALOR = 69
        )
        SELECT 
        ori.FCN_CUENTA,
        {int(sys.argv[2])} as FCN_ID_PERIDO,
        coalesce(p.FCC_VALOR, 0) as FTB_PENSION, 
        t.FCC_VALOR as  FTC_TIPO_CLIENTE,
        o.FCC_VALOR as FTC_ORIGEN,
        v.FCC_VALOR AS FTC_VIGENCIA,
        gen.FCC_VALOR AS FTC_GENERACION,
        NULL as FTO_INDICADORES,
        coalesce(b.FCC_VALOR, 0) AS FTC_BONO,
        FROM  indicador_origen ori
        LEFT JOIN database_generacion gen
        ON ori.FCN_CUENTA = gen.FCN_CUENTA
        LEFT JOIN indicador_tipo_cliente t
        on ori.FCN_CUENTA = t.FCN_CUENTA
        LEFT JOIN indicador_pension p 
        on ori.FCN_CUENTA = p.FCN_CUENTA
        LEFT JOIN indicador_vigencia v
        ON ori.FCN_CUENTA = v.FCN_CUENTA
        LEFT JOIN indicador_bono b
        ON ori.FCN_CUENTA = b.FCN_CUENTA
        """)
        df.show(2)
        _write_spark_dataframe(df,configure_postgres_spark,"TCHECHOS_CLIENTE")

