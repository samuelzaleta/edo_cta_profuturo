from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool, get_buc_pool
from profuturo.extraction import extract_dataset, extract_indicator
from profuturo.reporters import HtmlReporter
from sqlalchemy import text, Engine


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
buc_pool = get_buc_pool()
phase = 1

with define_extraction(phase, postgres_pool, buc_pool) as (postgres, buc):
    with register_time(postgres, phase):
        # Extracción
        truncate_table(postgres, 'tcdatmae_clientes')
        extract_dataset(buc, postgres, """
        SELECT C.NUMERO AS FTn_CUENTA,
               (PF.NOMBRE || ' ' || PF.APELLIDOPATERNO || ' ' ||PF.APELIDOMATERNO) AS FTC_NOMBRE,
               DI.CALLE AS FTC_CALLE,
               DI.NUMEROEXTERIOR AS FTC_NUMERO,
               ASE.NOMBRE AS FTC_COLONIA,
               CD.NOMBRE AS FTC_DELEGACION,
               CP.CODIGOPOSTAL AS FTN_CODIGOPOSTAL,
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
            LEFT JOIN IDENTIFICADOR NSS ON PF.IDPERSONA = NSS.IDPERSONA AND NSS.IDTIPOIDENTIFICADOR = 3 AND NSS.VALIDO = 1 AND NSS.ACTIVO = 1
            LEFT JOIN IDENTIFICADOR CURP ON PF.IDPERSONA = CURP.IDPERSONA AND CURP.IDTIPOIDENTIFICADOR = 2 AND CURP.VALIDO = 1 AND CURP.ACTIVO = 1
            LEFT JOIN IDENTIFICADOR RFC ON PF.IDPERSONA = RFC.IDPERSONA AND RFC.IDTIPOIDENTIFICADOR = 1 AND RFC.VALIDO = 1 AND RFC.ACTIVO = 1
        WHERE PCR.IDROL = 787 -- Rol cliente
          AND C.IDLINEANEGOCIO = 763 -- Linea de negocio
          AND D.IDTIPODOM = 818 -- Tipo de domicilio Particular
          AND D.IDSTATUSDOM = 761 -- ACTIVO
          AND D.PREFERENTE = 1 --Domicilio preferente
        """, "tcdatmae_clientes", phase, limit=100_000)

        # Indicadores
        indicators = postgres.execute(text("""
        SELECT ftn_id_indicador, ftc_descripcion 
        FROM tcgespro_indicadores
        WHERE ftc_bandera_estatus = 'V'
        """))

        for index, indicator in enumerate(indicators.fetchall()):
            print(f"Extracting {indicator[1]}...")

            indicators_queries = postgres.execute(text("""
            SELECT ftc_consulta_sql, ftc_bd_origen
            FROM tcgespro_indicador_consulta
            WHERE ftn_id_indicador = :indicator
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
                    extract_indicator(conn, postgres, indicator_query[0], index, phase, limit=100_000)

            print(f"Done extracting {indicator[1]}!")

        # Generación de cifras de control
        report = html_reporter.generate(postgres, """
        SELECT fto_indicadores->>'34' AS generacion,
               fto_indicadores->>'21' AS vigencia,
               CASE
                   WHEN fto_indicadores->>'3' = 'Asignado' THEN 'Asignado'
                   WHEN fto_indicadores->>'4' = 'Pensionado' THEN 'Pensionado'
                   WHEN fto_indicadores->>'3' = 'Afiliado' THEN 'Afiliado'
               END AS tipo_formato,
               fto_indicadores->>'31' AS tipo_cliente,
               COUNT(*)
        FROM tcdatmae_clientes
        GROUP BY fto_indicadores->>'34',
                 fto_indicadores->>'21',
                 CASE
                     WHEN fto_indicadores->>'3' = 'Asignado' THEN 'Asignado'
                     WHEN fto_indicadores->>'4' = 'Pensionado' THEN 'Pensionado'
                     WHEN fto_indicadores->>'3' = 'Afiliado' THEN 'Afiliado'
                 END,
                 fto_indicadores->>'31'
        """, ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación"], ["Clientes"])

        notify(
            postgres,
            "Cifras de control Cliente generadas",
            "Se han generado las cifras de control para clientes exitosamente",
            report,
            control=True,
        )
