from profuturo.common import register_time, define_extraction
from profuturo.database import get_postgres_pool, get_buc_pool
from profuturo.extraction import upsert_dataset, upsert_values_sentence
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
buc_pool = get_buc_pool()
phase = int(sys.argv[1])

query_upsert = f"""
INSERT INTO "TCDATMAE_CLIENTE"(
    "FTN_CUENTA", "FTC_NOMBRE", "FTC_AP_PATERNO", "FTC_AP_MATERNO",
    "FTC_CALLE", "FTC_NUMERO", "FTC_COLONIA", "FTC_DELEGACION",
    "FTN_CODIGO_POSTAL", "FTC_ENTIDAD_FEDERATIVA", "FTC_NSS",
    "FTC_CURP", "FTC_RFC"
)
VALUES {upsert_values_sentence(lambda i: [
    f":id_{i}", f":name_{i}", f":middle_name_{i}", f":last_name_{i}",
    f":street_{i}", f":street_number_{i}", f":colony_{i}", f":municipality_{i}",
    f":zip_{i}", f":state_{i}", f":nss_{i}",
    f":curp_{i}", f":rfc_{i}",
])}
ON CONFLICT ("FTN_CUENTA") DO UPDATE 
SET "FTC_NOMBRE" = EXCLUDED."FTC_NOMBRE", "FTC_AP_PATERNO" = EXCLUDED."FTC_AP_PATERNO", 
    "FTC_AP_MATERNO" = EXCLUDED."FTC_AP_MATERNO", "FTC_CALLE" = EXCLUDED."FTC_CALLE", 
    "FTC_NUMERO" = EXCLUDED."FTC_NUMERO", "FTC_COLONIA" = EXCLUDED."FTC_COLONIA", 
    "FTC_DELEGACION" = EXCLUDED."FTC_DELEGACION", "FTN_CODIGO_POSTAL" = EXCLUDED."FTN_CODIGO_POSTAL", 
    "FTC_ENTIDAD_FEDERATIVA" = EXCLUDED."FTC_ENTIDAD_FEDERATIVA", "FTC_NSS" = EXCLUDED."FTC_NSS", 
    "FTC_CURP" = EXCLUDED."FTC_CURP", "FTC_RFC" = EXCLUDED."FTC_RFC"
"""

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
          -- AND D.IDSTATUSDOM = 761 ACTIVO
          -- AND D.PREFERENTE = 1 Domicilio preferente
        """, query_upsert, "TCDATMAE_CLIENTE")
