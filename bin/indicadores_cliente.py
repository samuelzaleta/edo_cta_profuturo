from profuturo.common import register_time, define_extraction, truncate_table, notify
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from pyspark.sql.functions import lit, to_json, struct
import sys
import json

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])


with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id):
        spark = _get_spark_session()

        # Extracción
        truncate_table(postgres, "TCHECHOS_CLIENTE", term=term_id)
        read_table_insert_temp_view(configure_mit_spark, """
        SELECT DISTINCT(IND.FTN_NUM_CTA_INVDUAL) AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '1' THEN 'TRUE'
                   WHEN '0' THEN 'FALSE'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI = 11
        AND FTC_VIGENCIA= 1
        """, "indicador_pension")
        spark.sql("select count(*) as count_indicador_pension from indicador_pension").show()

        read_table_insert_temp_view(configure_mit_spark, """
        SELECT DISTINCT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '66' THEN 'IMSS'
                   WHEN '67' THEN 'ISSSTE'
                   WHEN '68' THEN 'INDEPENDIENTE'
                   WHEN '69' THEN 'MIXTO'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI = 1
          AND FTC_VIGENCIA= 1
        """, "indicador_origen")
        spark.sql("select count(*) as count_indicador_origen from indicador_origen").show()
        spark.sql("select * from indicador_origen").show(20)

        read_table_insert_temp_view(configure_mit_spark, """
        SELECT DISTINCT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '713' THEN 'Asignado'
                   WHEN '714' THEN 'Afiliado'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI = 12
          AND FTC_VIGENCIA= 1
        """, "indicador_tipo_cliente")
        spark.sql("select count(*) as count_indicador_tipo_cliente from indicador_tipo_cliente").show()
        spark.sql("select * from indicador_tipo_cliente").show(20)

        read_table_insert_temp_view(configure_mit_spark, """
        SELECT DISTINCT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND 
                   WHEN '1' THEN 'V'
                   WHEN '0' THEN 'N'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI = 2
          AND FTC_VIGENCIA= 1
        """, "indicador_vigencia")
        spark.sql("select count(*) as count_indicador_vigencia from indicador_vigencia").show()

        read_table_insert_temp_view(configure_mit_spark, """
        SELECT DISTINCT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, 'TRUE' /* TRUE */ AS FCC_VALOR
        FROM CIERREN.THAFOGRAL_SALDO_HISTORICO_V2
        WHERE FCN_ID_SIEFORE = 81
        """, "indicador_bono")
        spark.sql("select count(*) as count_indicador_bono from indicador_bono").show()

        read_table_insert_temp_view(configure_mit_spark, """
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               'AFORE' /*2 AFORE */ AS FCC_VALOR
        FROM cierren.thafogral_saldo_historico_v2 SH
        INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR LIKE '%SAR%' OR TS.FCC_VALOR LIKE '%92%'
        """, "generacion_afore")
        spark.sql("select count(*) as count_generacion_afore from generacion_afore").show()

        read_table_insert_temp_view(configure_mit_spark, """
        WITH TIPO_SUBCUENTA AS (
            SELECT DISTINCT TS.FCN_ID_TIPO_SUBCTA, C.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA TS
            INNER JOIN THCRXGRAL_CAT_CATALOGO C ON TS.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        )
        SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               'TRANSICION' /* 3 TRANSICION */ AS FCC_VALOR
        FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN TIPO_SUBCUENTA TS ON TS.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        WHERE TS.FCC_VALOR NOT LIKE '%SAR%' OR TS.FCC_VALOR NOT LIKE '%92%'
        """, "generacion_transicion")
        spark.sql("select count(*) as count_generacion_transicion from generacion_transicion").show()

        df = spark.sql(f"""
        WITH indicador_generacion AS (
            SELECT FCN_CUENTA, FCC_VALOR 
            FROM generacion_afore ga
            
            UNION ALL
            
            SELECT FCN_CUENTA, FCC_VALOR 
            FROM generacion_transicion gt
            
            UNION ALL
            
            SELECT FCN_CUENTA, 'MIXTO' /* 4 MIXTO */ AS FCC_VALOR 
            FROM indicador_origen ori
            WHERE FCC_VALOR = 69
        )
        SELECT DISTINCT o.FCN_CUENTA,
               {term_id} AS FCN_ID_PERIODO,
               coalesce(cast(p.FCC_VALOR AS BOOLEAN), cast('FALSE' as BOOLEAN)) AS FTB_PENSION, 
               t.FCC_VALOR AS  FTC_TIPO_CLIENTE,
               o.FCC_VALOR AS FTC_ORIGEN,
               v.FCC_VALOR AS FTC_VIGENCIA,
               g.FCC_VALOR AS FTC_GENERACION,
               coalesce(cast(p.FCC_VALOR AS BOOLEAN), cast('FALSE' as BOOLEAN))  AS FTB_BONO
               --JSON_OBJECT('Vigencia', v.FCC_VALOR, 'Generacion', g.FCC_VALOR) AS FTO_INDICADORES
        FROM indicador_origen o
            LEFT JOIN indicador_generacion g ON o.FCN_CUENTA = g.FCN_CUENTA
            LEFT JOIN indicador_tipo_cliente t ON o.FCN_CUENTA = t.FCN_CUENTA
            LEFT JOIN indicador_pension p ON o.FCN_CUENTA = p.FCN_CUENTA
            LEFT JOIN indicador_vigencia v ON o.FCN_CUENTA = v.FCN_CUENTA
            LEFT JOIN indicador_bono b ON o.FCN_CUENTA = b.FCN_CUENTA
        """)
        df = df.withColumn("FTO_INDICADORES", to_json(struct(lit('{}'))))
        df.show(2)
        df = df.dropDuplicates(["FCN_CUENTA"])
        _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TCHECHOS_CLIENTE"')

        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
             SELECT I."FTC_GENERACION" AS GENERACION,
               I."FTC_VIGENCIA" AS VIGENCIA,
               I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
               I."FTC_ORIGEN" AS ORIGEN,
               COUNT(DISTINCT I."FCN_CUENTA") AS CLIENTES
        FROM "HECHOS"."TCHECHOS_CLIENTE" I
         WHERE I."FCN_ID_PERIODO" = 27
        GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
             """,
            ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación"],
            ["Clientes"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Clientes ingestados",
            "Se han ingestado los clientes de forma exitosa",
            report,
            term=term_id,
        )
