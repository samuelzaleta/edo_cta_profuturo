from profuturo.common import truncate_table, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_dataset

postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = 0

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    with register_time(postgres, phase):
        truncate_table(postgres, 'tcdatmae_tipos_subcuenta')
        extract_dataset(mit, postgres, """
        SELECT DISTINCT sct.FCN_ID_TIPO_SUBCTA AS ftn_id_tipo_subcta, 
               sct.FCN_ID_REGIMEN, sct.FCN_ID_CAT_SUBCTA, cat.FCC_VALOR
        FROM TCCRXGRAL_TIPO_SUBCTA sct
        INNER JOIN THCRXGRAL_CAT_CATALOGO cat ON sct.FCN_ID_CAT_SUBCTA = cat.FCN_ID_CAT_CATALOGO
        """, "tcdatmae_tipos_subcuenta", phase)

        truncate_table(postgres, 'catalogo_siefores')
        extract_dataset(mit, postgres, """
        WITH dataset AS (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_CAT_CATALOGO ORDER BY FHD_HIST_FEH_CRE DESC) AS ROW_NUM,
                   FHD_HIST_FEH_CRE, FCN_ID_CAT_CATALOGO,
                   FCN_ID_TIPO_CAT, FCC_VALOR, FCC_DESC,
                   FCB_VIGENCIA
            FROM THCRXGRAL_CAT_CATALOGO
            WHERE FCN_ID_TIPO_CAT = 18 AND FCB_VIGENCIA = 1
        )
        SELECT FHD_HIST_FEH_CRE, FCN_ID_CAT_CATALOGO,
               FCN_ID_TIPO_CAT, FCC_VALOR, FCC_DESC,
               FCB_VIGENCIA
        FROM dataset
        WHERE ROW_NUM = 1
        """, "catalogo_siefores", phase)
