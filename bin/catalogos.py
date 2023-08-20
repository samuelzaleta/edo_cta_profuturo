from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import upsert_dataset
from profuturo.extraction import extract_terms
import sys


postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]

    with register_time(postgres_pool, phase, term_id):
        upsert_dataset(mit, postgres, """
        SELECT S.FCN_ID_SIEFORE AS id, C.FCC_VALOR AS description
        FROM TCCRXGRAL_SIEFORE S
        INNER JOIN TCCRXGRAL_CAT_CATALOGO C ON S.FCN_ID_SIEFORE = C.FCN_ID_CAT_CATALOGO
        """, """
        INSERT INTO "TCDATMAE_SIEFORE"("FTN_ID_SIEFORE", "FTC_DESCRIPCION_CORTA")
        VALUES (...)
        ON CONFLICT ("FTN_ID_SIEFORE") DO UPDATE 
        SET "FTC_DESCRIPCION_CORTA" = EXCLUDED."FTC_DESCRIPCION_CORTA"
        """, lambda i: [f":id_{i}", f":description_{i}"], "TCDATMAE_SIEFORE")
        upsert_dataset(mit, postgres, """
        SELECT S.FCN_ID_TIPO_SUBCTA AS id, S.FCN_ID_REGIMEN AS regime_id, S.FCN_ID_CAT_SUBCTA AS subacc_cat_id, 
               C.FCC_VALOR AS description
        FROM TCCRXGRAL_TIPO_SUBCTA S
        INNER JOIN TCCRXGRAL_CAT_CATALOGO C ON S.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        """, """
        INSERT INTO "TCDATMAE_TIPO_SUBCUENTA"("FTN_ID_TIPO_SUBCTA", "FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA", "FCC_VALOR")
        VALUES (...)
        ON CONFLICT ("FTN_ID_TIPO_SUBCTA") DO UPDATE 
        SET "FCN_ID_REGIMEN" = EXCLUDED."FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA" = EXCLUDED."FCN_ID_CAT_SUBCTA", 
            "FCC_VALOR" = EXCLUDED."FCC_VALOR"
        """, lambda i: [f":id_{i}", f":regime_id_{i}", f":subacc_cat_id_{i}", f":description_{i}"], "TCDATMAE_TIPO_SUBCUENTA")

        upsert_dataset(mit, postgres, """
        SELECT FFN_ID_CONCEPTO_MOV AS cod_mov, 0 AS monpes, S.FCN_ID_TIPO_SUBCTA AS tipo_subcta,
               FFC_DESCRIPCION_MIT AS description
        FROM CIERREN.TFCRXGRAL_CONFIG_MOV_ITGY M
            INNER JOIN TRAFOGRAL_MOV_SUBCTA S ON M.FRN_ID_MOV_SUBCTA = S.FRN_ID_MOV_SUBCTA
        WHERE FFN_COD_MOV_ITGY IS NOT NULL AND FFN_POSICION_ITGY IS NOT NULL
        """, """
        INSERT INTO "TCGESPRO_MOVIMIENTO_PROFUTURO"(
            "FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES", "FCN_ID_TIPO_SUBCUENTA", "FTC_ORIGEN", "FTC_DESCRIPCION"
        )
        VALUES (...)
        ON CONFLICT ("FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES") DO UPDATE 
        SET "FCN_ID_TIPO_SUBCUENTA" = EXCLUDED."FCN_ID_TIPO_SUBCUENTA", "FTC_ORIGEN" = EXCLUDED."FTC_ORIGEN",
            "FTC_DESCRIPCION" = EXCLUDED."FTC_DESCRIPCION"
        """, lambda i: [f":cod_mov_{i}", f":monpes_{i}", f":tipo_subcta_{i}", "'INTEGRITY'", f":description_{i}"], "TCGESPRO_MOVIMIENTO_PROFUTURO")

        notify(
            postgres,
            "Catálogos ingestados",
            "Se han ingestado los catálogos de forma exitosa",
            term=term_id,
        )
