from profuturo.common import truncate_table, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_dataset, extract_terms, upsert_dataset

postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = 5

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    with register_time(postgres, phase):
        upsert_dataset(mit, postgres, """
        WITH dataset AS (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_CAT_CATALOGO ORDER BY FHD_HIST_FEH_CRE DESC) AS ROW_NUM,
                   FCN_ID_CAT_CATALOGO, FCC_DESC
            FROM THCRXGRAL_CAT_CATALOGO
            WHERE FCN_ID_TIPO_CAT = 18 AND FCB_VIGENCIA = 1
        )
        SELECT FCN_ID_CAT_CATALOGO AS id, FCC_DESC AS description
        FROM dataset
        WHERE ROW_NUM = 1
        """, """
        INSERT INTO "TCDATMAE_SIEFORE"("FTN_ID_SIEFORE", "FTC_DESCRIPCION_CORTA")
        VALUES (:id, :description)
        ON CONFLICT ("FTN_ID_SIEFORE") DO UPDATE 
        SET "FTC_DESCRIPCION_CORTA" = :description
        """, "TCDATMAE_SIEFORE")
        upsert_dataset(mit, postgres, """
        SELECT DISTINCT sct.FCN_ID_TIPO_SUBCTA AS id, 
               sct.FCN_ID_REGIMEN AS regime_id, 
               sct.FCN_ID_CAT_SUBCTA AS subacc_cat_id, 
               cat.FCC_VALOR AS description
        FROM TCCRXGRAL_TIPO_SUBCTA sct
        INNER JOIN THCRXGRAL_CAT_CATALOGO cat ON sct.FCN_ID_CAT_SUBCTA = cat.FCN_ID_CAT_CATALOGO
        """, """
        INSERT INTO "TCDATMAE_TIPO_SUBCUENTA"("FTN_ID_TIPO_SUBCTA", "FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA", "FCC_VALOR")
        VALUES (:id, :regime_id, :subacc_cat_id, :description)
        ON CONFLICT ("FTN_ID_TIPO_SUBCTA") DO UPDATE 
        SET "FCN_ID_REGIMEN" = :regime_id, "FCN_ID_CAT_SUBCTA" = :subacc_cat_id, "FCC_VALOR" = :description
        """, "TCDATMAE_TIPO_SUBCUENTA")
