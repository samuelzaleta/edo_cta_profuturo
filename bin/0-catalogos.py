from sqlalchemy import text
from profuturo.common import truncate_table, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_dataset, extract_terms

postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = 5

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    with register_time(postgres, phase):
        cursor = mit.execute(text("""
        WITH dataset AS (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_CAT_CATALOGO ORDER BY FHD_HIST_FEH_CRE DESC) AS ROW_NUM,
                   FCN_ID_CAT_CATALOGO, FCC_DESC
            FROM THCRXGRAL_CAT_CATALOGO
            WHERE FCN_ID_TIPO_CAT = 18 AND FCB_VIGENCIA = 1
        )
        SELECT FCN_ID_CAT_CATALOGO, FCC_DESC
        FROM dataset
        WHERE ROW_NUM = 1
        """))

        for row in cursor.fetchall():
            postgres.execute(text("""
            INSERT INTO "TCDATMAE_SIEFORE"("FTN_ID_SIEFORE", "FTC_DESCRIPCION_CORTA")
            VALUES (:id, :description)
            ON CONFLICT ("FTN_ID_SIEFORE") DO UPDATE 
            SET "FTC_DESCRIPCION_CORTA" = :description
            """), {"id": row[0], "description": row[1]})

    terms = extract_terms(postgres, phase)
    for term in terms:
        term_id = term["id"]
        start_month = term["start_month"]
        end_month = term["end_month"]

        with register_time(postgres, phase, term=term_id):
            truncate_table(postgres, 'TCDATMAE_TIPO_SUBCUENTA', term=term_id)
            extract_dataset(mit, postgres, """
            SELECT DISTINCT sct.FCN_ID_TIPO_SUBCTA AS FTN_ID_TIPO_SUBCTA, 
                   sct.FCN_ID_REGIMEN, sct.FCN_ID_CAT_SUBCTA, cat.FCC_VALOR
            FROM TCCRXGRAL_TIPO_SUBCTA sct
            INNER JOIN THCRXGRAL_CAT_CATALOGO cat ON sct.FCN_ID_CAT_SUBCTA = cat.FCN_ID_CAT_CATALOGO
            """, "TCDATMAE_TIPO_SUBCUENTA", term=term_id)
