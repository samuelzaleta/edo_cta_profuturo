from profuturo.common import register_time, define_extraction
from profuturo.database import get_postgres_pool, get_buc_pool
from profuturo.extraction import upsert_dataset
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
buc_pool = get_buc_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, buc_pool) as (postgres, buc):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id):
        # Extracción
        upsert_dataset(buc, postgres, """
        SELECT C.NUMERO AS id,
               PF.NOMBRE AS name,
               PF.APELLIDOPATERNO AS middle_name,
               PF.APELIDOMATERNO AS last_name,
               DI.CALLE AS street,
               DI.NUMEROEXTERIOR AS street_number,
               ASE.NOMBRE AS colony,
               CD.NOMBRE AS municipality,
               M.NOMBRE AS delegacion,
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
            "FTN_CUENTA", "FTC_NOMBRE", "FTC_AP_PATERNO", "FTC_AP_MATERNO",
            "FTC_CALLE", "FTC_NUMERO", "FTC_COLONIA", "FTC_DELEGACION",
            "FTN_CODIGO_POSTAL", "FTC_ENTIDAD_FEDERATIVA", "FTC_NSS",
            "FTC_CURP", "FTC_RFC"
        )
        VALUES (
            :id, :name, :middle_name, :last_name, 
            :street, :street_number, :colony, 
            :municipality, :zip, :state, :nss, 
            :curp, :rfc
        )
        ON CONFLICT ("FTN_CUENTA") DO UPDATE 
        SET "FTC_NOMBRE" = :name, "FTC_AP_PATERNO" = :middle_name, "FTC_AP_MATERNO" = :last_name, 
            "FTC_CALLE" = :street, "FTC_NUMERO" = :street_number, "FTC_COLONIA" = :colony, 
            "FTC_DELEGACION" = :municipality, "FTN_CODIGO_POSTAL" = :zip, "FTC_ENTIDAD_FEDERATIVA" = :state, 
            "FTC_NSS" = :nss, "FTC_CURP" = :curp, "FTC_RFC" = :rfc
        """, "TCDATMAE_CLIENTE", limit=1_000_000)
