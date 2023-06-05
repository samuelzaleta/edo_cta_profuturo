from profuturo.common import notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool, get_buc_pool
from profuturo.extraction import extract_indicator, upsert_dataset
from profuturo.reporters import HtmlReporter
from sqlalchemy import text, Engine


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
buc_pool = get_buc_pool()
phase = 6

with define_extraction(phase, postgres_pool, buc_pool) as (postgres, buc):
    with register_time(postgres, phase):
        # Extracción
        upsert_dataset(buc, postgres, """
        SELECT C.NUMERO AS id,
               (PF.NOMBRE || ' ' || PF.APELLIDOPATERNO || ' ' ||PF.APELIDOMATERNO) AS name,
               DI.CALLE AS street,
               DI.NUMEROEXTERIOR AS street_number,
               ASE.NOMBRE AS colony,
               CD.NOMBRE AS municipality,
               CP.CODIGOPOSTAL AS zip,
               E.NOMBRE AS state,
               NSS.VALOR_IDENTIFICADOR AS nss,
               CURP.VALOR_IDENTIFICADOR AS curp,
               RFC.VALOR_IDENTIFICADOR AS rfc
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
          AND D.PREFERENTE = 1 -- Domicilio preferente
        """, """
        INSERT INTO "TCDATMAE_CLIENTE"(
            "FTN_CUENTA", "FTC_NOMBRE", "FTC_CALLE", "FTC_NUMERO", 
            "FTC_COLONIA", "FTC_DELEGACION", "FTN_CODIGO_POSTAL", 
            "FTC_ENTIDAD_FEDERATIVA", "FTC_NSS", "FTC_CURP", "FTC_RFC"
        )
        VALUES (
            :id, :name, :street, :street_number,
            :colony, :municipality,:zip,
            :state, :nss, :curp, :rfc
        )
        ON CONFLICT ("FTN_CUENTA") DO UPDATE 
        SET "FTC_NOMBRE" = :name, "FTC_CALLE" = :street, "FTC_NUMERO" = :street_number,
            "FTC_COLONIA" = :colony, "FTC_DELEGACION" = :municipality, "FTN_CODIGO_POSTAL" = :zip,
            "FTC_ENTIDAD_FEDERATIVA" = :state, "FTC_NSS" = :nss, "FTC_CURP" = :curp, "FTC_RFC" = :rfc
        """, "TCDATMAE_CLIENTE", limit=100_000)

        # Indicadores
        postgres.execute(text('UPDATE "TCDATMAE_CLIENTE" SET "FTO_INDICADORES" = \'{}\''))

        indicators = postgres.execute(text("""
        SELECT "FTN_ID_INDICADOR", "FTC_DESCRIPCION" 
        FROM "TCGESPRO_INDICADOR"
        WHERE "FTC_BANDERA_ESTATUS" = 'V'
        """))

        for index, indicator in enumerate(indicators.fetchall()):
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
                    extract_indicator(conn, postgres, indicator_query[0], index, limit=100_000)

            print(f"Done extracting {indicator[1]}!")

        # Generación de cifras de control
        report = html_reporter.generate(postgres, """
        SELECT "FTO_INDICADORES"->>'34' AS generacion,
               "FTO_INDICADORES"->>'21' AS vigencia,
               CASE
                   WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                   WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                   WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
               END AS tipo_formato,
               "FTO_INDICADORES"->>'31' AS tipo_cliente,
               COUNT(*)
        FROM "TCDATMAE_CLIENTE"
        GROUP BY "FTO_INDICADORES"->>'34',
                 "FTO_INDICADORES"->>'21',
                 CASE
                     WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                     WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                     WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                 END,
                 "FTO_INDICADORES"->>'31'
        """, ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación"], ["Clientes"])

        notify(
            postgres,
            "Cifras de control Cliente generadas",
            "Se han generado las cifras de control para clientes exitosamente",
            report,
            control=True,
        )
