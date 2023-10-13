from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import get_postgres_pool, get_buc_pool, configure_buc_spark, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from datetime import datetime
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
buc_pool = get_buc_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, buc_pool) as (postgres, buc):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):

        truncate_table(postgres, "TCDATMAE_CLIENTE")
        # Extracción
        query = """
        SELECT
            FTN_CUENTA,
            FTC_NOMBRE,
            FTC_AP_PATERNO,
            FTC_AP_MATERNO,
            FTC_CALLE,
            FTC_NUMERO,
            FTC_COLONIA,
            FTC_DELEGACION,
            FTN_CODIGO_POSTAL,
            FTC_ENTIDAD_FEDERATIVA,
            FTC_NSS,
            FTC_CURP,
            FTC_RFC 
        FROM (
            SELECT
               ROW_NUMBER() over (partition by TO_NUMBER(REGEXP_REPLACE(TO_CHAR(C.NUMERO), '[^0-9]', '')) order by C.NUMERO) AS id,
               TO_NUMBER(REGEXP_REPLACE(TO_CHAR(C.NUMERO), '[^0-9]', '')) AS FTN_CUENTA,
               PF.NOMBRE AS FTC_NOMBRE,
               PF.APELLIDOPATERNO AS FTC_AP_PATERNO,
               PF.APELIDOMATERNO AS FTC_AP_MATERNO,
               DI.CALLE AS FTC_CALLE,
               DI.NUMEROEXTERIOR AS FTC_NUMERO,
               ASE.NOMBRE AS FTC_COLONIA,
               CD.NOMBRE AS FTC_DELEGACION,
               CAST(CP.CODIGOPOSTAL AS INTEGER) AS FTN_CODIGO_POSTAL,
               E.NOMBRE AS FTC_ENTIDAD_FEDERATIVA,
               NSS.VALOR_IDENTIFICADOR AS FTC_NSS,
               CURP.VALOR_IDENTIFICADOR AS FTC_CURP,
               RFC.VALOR_IDENTIFICADOR AS FTC_RFC
            FROM CONTRATO C
                INNER JOIN PERSONA_CONT_ROL PCR ON C.IDCONTRATO = PCR.IDCONTRATO
                INNER JOIN PERSONA_FISICA PF ON PCR.IDPERSONA = PF.IDPERSONA
                INNER JOIN DOMICILIO D ON PF.IDPERSONA = D.IDPERSONA
                INNER JOIN DIRECCION DI ON D.IDDIRECCION = DI.IDDIRECCION
                LEFT JOIN CODIGO_POSTAL CP ON DI.IDCODIGOPOSTAL = CP.IDCODIGOPOSTAL
                LEFT JOIN ESTADO E ON CP.IDESTADO = E.IDESTADO
                LEFT JOIN MUNICIPIO M ON CP.IDMUNICIPIO = M.IDMUNICIPIO
                LEFT JOIN CIUDAD CD ON CP.IDCIUDAD = CD.IDCIUDAD
                LEFT JOIN ASENTAMIENTO ASE ON DI.IDASENTAMIENTO = ASE.IDASENTAMIENTO
                LEFT JOIN IDENTIFICADOR NSS ON PF.IDPERSONA = NSS.IDPERSONA AND NSS.IDTIPOIDENTIFICADOR = 3 
                                           AND NSS.VALIDO = 1 AND NSS.ACTIVO = 1
                LEFT JOIN IDENTIFICADOR CURP ON PF.IDPERSONA = CURP.IDPERSONA AND CURP.IDTIPOIDENTIFICADOR = 2 
                                            AND CURP.VALIDO = 1 AND CURP.ACTIVO = 1
                LEFT JOIN IDENTIFICADOR RFC ON PF.IDPERSONA = RFC.IDPERSONA AND RFC.IDTIPOIDENTIFICADOR = 1 
                                           AND RFC.VALIDO = 1 AND RFC.ACTIVO = 1
            WHERE PCR.IDROL = 787 -- Rol cliente
              AND C.IDLINEANEGOCIO = 763 -- Linea de negocio
              AND D.IDTIPODOM = 818 -- Tipo de domicilio Particular
              -- AND D.IDSTATUSDOM = 761 ACTIVO
              -- AND D.PREFERENTE = 1 Domicilio preferente
        ) where id = 1
        """
        read_table_insert_temp_view(
            configure_buc_spark,
            query,
            'cliente',
        )
        # Extracción
        truncate_table(postgres, "TCHECHOS_CLIENTE_INDICADOR", term=term_id)
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
        SELECT pg.FTN_NUM_CTA_INVDUAL, S.FCC_VALOR
        FROM BENEFICIOS.TTCRXGRAL_PAGO PG
            INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO S ON PG.FCN_ID_SUBPROCESO = S.FCN_ID_CAT_CATALOGO
            INNER JOIN BENEFICIOS.TTCRXGRAL_PAGO_SUBCTA PGS ON PGS.FTC_FOLIO = PG.FTC_FOLIO AND PGS.FTC_FOLIO_LIQUIDACION = PG.FTC_FOLIO_LIQUIDACION
        WHERE PG.FCN_ID_PROCESO IN (4050, 4051)
          AND PG.FCN_ID_SUBPROCESO IN (309, 310)
          AND PGS.FCN_ID_TIPO_SUBCTA NOT IN (15, 16, 17, 18)
          AND PG.FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "indicador_tipo_pension", {'start': start_month, 'end': end_month})
        spark.sql("select count(*) as count_tipo_pension from indicador_tipo_pension").show()

        read_table_insert_temp_view(configure_mit_spark, """
        SELECT P.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, C.FCC_DESC AS FCC_VALOR
        FROM TTAFOGRAL_OSS P
        INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO C ON P.FCN_ID_GRUPO = C.FCN_ID_CAT_CATALOGO
        """, "indicador_perfil_inversion")
        spark.sql("select count(*) as count_perfil_inversion from indicador_perfil_inversion").show()

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

        df = spark.sql("""
        SELECT *
        FROM cliente
        """)
        df.show()
        _write_spark_dataframe(df, configure_postgres_spark, '"MAESTROS"."TCDATMAE_CLIENTE"')

        df = spark.sql(f"""
        WITH indicador_generacion AS (
            SELECT 
            DISTINCT 
            X.FCN_CUENTA,
            COALESCE(Y.FCC_VALOR, X.FCC_VALOR) AS FCC_VALOR
            FROM (
            SELECT FCN_CUENTA, FCC_VALOR 
            FROM generacion_afore ga
            UNION ALL
            SELECT FCN_CUENTA, FCC_VALOR 
            FROM generacion_transicion gt
            ) X
            LEFT JOIN (
            SELECT DISTINCT FCN_CUENTA, 'MIXTO' /* 4 MIXTO */ AS FCC_VALOR 
            FROM indicador_origen ori
            WHERE FCC_VALOR = 'MIXTO') Y 
            ON X.FCN_CUENTA = Y.FCN_CUENTA
        )
        SELECT DISTINCT o.FCN_CUENTA,
               {term_id} AS FCN_ID_PERIODO,
               coalesce(cast(p.FCC_VALOR AS BOOLEAN), false) AS FTB_PENSION, 
               t.FCC_VALOR AS  FTC_TIPO_CLIENTE,
               o.FCC_VALOR AS FTC_ORIGEN,
               v.FCC_VALOR AS FTC_VIGENCIA,
               g.FCC_VALOR AS FTC_GENERACION,
               coalesce(cast(p.FCC_VALOR AS BOOLEAN), false)  AS FTB_BONO,
               tp.FCC_VALOR AS FTC_TIPO_PENSION,
               i.FCC_VALOR AS FTC_PERFIL_INVERSION
               --JSON_OBJECT('Vigencia', v.FCC_VALOR, 'Generacion', g.FCC_VALOR) AS FTO_INDICADORES
        FROM indicador_origen o
            LEFT JOIN indicador_generacion g ON o.FCN_CUENTA = g.FCN_CUENTA
            LEFT JOIN indicador_tipo_cliente t ON o.FCN_CUENTA = t.FCN_CUENTA
            LEFT JOIN indicador_pension p ON o.FCN_CUENTA = p.FCN_CUENTA
            LEFT JOIN indicador_vigencia v ON o.FCN_CUENTA = v.FCN_CUENTA
            LEFT JOIN indicador_bono b ON o.FCN_CUENTA = b.FCN_CUENTA
            LEFT JOIN indicador_tipo_pension tp ON o.FCN_CUENTA = p.FCN_CUENTA
            LEFT JOIN indicador_perfil_inversion i ON o.FCN_CUENTA = i.FCN_CUENTA
        WHERE o.FCN_CUENTA IN (SELECT DISTINCT FTN_CUENTA FROM cliente)
        """)
        #df = df.withColumn("FTO_INDICADORES", to_json(struct(lit('{}'))))
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
             WHERE I."FCN_ID_PERIODO" = :term
             GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
             """,
            ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación"],
            ["Clientes"],
            params={"term": term_id},
        )

        notify(
            postgres,
            f"Clientes ingestados",
            phase,
            area,
            term=term_id,
            message=f"Se han ingestado los clientes de forma exitosa para el periodo {time_period}",
            details=report,
        )