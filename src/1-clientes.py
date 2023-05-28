from common import MemoryLogger, HtmlReporter, get_postgres_pool, get_mit_pool, get_buc_pool, truncate_table, extract_terms, extract_dataset, extract_indicator, notify
from sqlalchemy import text, Engine


app_logger = MemoryLogger()
html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
buc_pool = get_buc_pool()

with postgres_pool.begin() as postgres:
    terms = extract_terms(postgres)

    with buc_pool.begin() as buc:
        for term in terms:
            term_id = term["id"]
            start_month = term["start_month"]
            end_month = term["end_month"]

            # Extracción
            truncate_table(postgres, 'tcdatmae_clientes')
            extract_dataset(buc, postgres, """
            SELECT C.NUMERO AS CUENTA,
                   (PF.NOMBRE || ' ' || PF.APELLIDOPATERNO || ' ' ||PF.APELIDOMATERNO) AS NOMBRE,
                   DI.CALLE,
                   DI.NUMEROEXTERIOR AS NUMERO,
                   ASE.NOMBRE AS COLONIA,
                   CD.NOMBRE AS DELEGACION,
                   CP.CODIGOPOSTAL,
                   E.NOMBRE AS ENTIDAD_FEDERATIVA,
                   NSS.VALOR_IDENTIFICADOR AS NSS,
                   CURP.VALOR_IDENTIFICADOR AS CURP,
                   RFC.VALOR_IDENTIFICADOR AS RFC
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
            """, "tcdatmae_clientes", term_id, phase=1, limit=100_000, with_term=False)

            # Indicadores
            indicators = postgres.execute(text("""
            SELECT ftn_id_indicador, ftc_descripcion 
            FROM tcgespro_indicadores
            WHERE ftc_bandera_estatus = 'V'
            """))

            for i, indicator in enumerate(indicators.fetchall()):
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
                        extract_indicator(conn, postgres, indicator_query[0], index=i, phase=1)

            # Generación de cifras de control
            report = html_reporter.generate(postgres, """
            SELECT ftc_generacion,
                   ftc_vigencia,
                   CASE
                       WHEN ftc_origen = 'Asignado' THEN 'Asignado'
                       WHEN ftb_pension = true AND ftb_bono = true THEN 'Pensionado con Bono'
                       WHEN ftb_pension = true THEN 'Pensionado'
                       WHEN ftc_origen = 'Afiliado' AND ftb_bono = true THEN 'Afiliado con Bono'
                       WHEN ftc_origen = 'Afiliado' THEN 'Afiliado'
                   END AS tipo_formato,
                   ftc_tipo_cliente,
                   COUNT(*)
            FROM tcdatmae_clientes
            GROUP BY ftc_generacion,
                     ftc_vigencia,
                     CASE
                         WHEN ftc_origen = 'Asignado' THEN 'Asignado'
                         WHEN ftb_pension = true AND ftb_bono = true THEN 'Pensionado con Bono'
                         WHEN ftb_pension = true THEN 'Pensionado'
                         WHEN ftc_origen = 'Afiliado' AND ftb_bono = true THEN 'Afiliado con Bono'
                         WHEN ftc_origen = 'Afiliado' THEN 'Afiliado'
                     END,
                     ftc_tipo_cliente
            """, ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación"], ["Clientes"])

            notify(
                postgres,
                term_id,
                "Cifras de control Cliente generadas",
                "Se han generado las cifras de control para clientes exitosamente",
                report,
                control=True,
            )
