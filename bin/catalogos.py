from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_mit_pool, configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool, configure_mitedocta_spark
from profuturo.extraction import upsert_dataset, extract_terms, _get_spark_session, _write_spark_dataframe, _create_spark_dataframe
import sys

postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):

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
               C.FCC_VALOR AS description, CC.FCC_VALOR as description_siefore
        FROM TCCRXGRAL_TIPO_SUBCTA S
        INNER JOIN TCCRXGRAL_CAT_CATALOGO C ON S.FCN_ID_CAT_SUBCTA = C.FCN_ID_CAT_CATALOGO
        INNER JOIN TCCRXGRAL_CAT_CATALOGO CC ON S.FCN_ID_REGIMEN = C.FCN_ID_CAT_CATALOGO
        """, """
        INSERT INTO "TCDATMAE_TIPO_SUBCUENTA"("FTN_ID_TIPO_SUBCTA", "FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA", "FCC_VALOR", "FTC_TIPO_CLIENTE")
        VALUES (...)
        ON CONFLICT ("FTN_ID_TIPO_SUBCTA") DO UPDATE 
        SET "FCN_ID_REGIMEN" = EXCLUDED."FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA" = EXCLUDED."FCN_ID_CAT_SUBCTA", 
            "FCC_VALOR" = EXCLUDED."FCC_VALOR", "FTC_TIPO_CLIENTE" = ECLUEDED."FTC_TIPO_CLIENTE"
        """, lambda i: [f":id_{i}", f":regime_id_{i}", f":subacc_cat_id_{i}", f":description_{i}", f"description_siefore_{i}"],
                       "TCDATMAE_TIPO_SUBCUENTA")

        upsert_dataset(mit, postgres, """
        SELECT FFN_ID_CONCEPTO_MOV AS cod_mov,
               0 AS monpes,
               S.FCN_ID_TIPO_SUBCTA AS tipo_subcta,
               M.FFC_DESCRIPCION_MIT AS description
        FROM CIERREN.TFCRXGRAL_CONFIG_MOV_ITGY M
        INNER JOIN TRAFOGRAL_MOV_SUBCTA S ON M.FRN_ID_MOV_SUBCTA = S.FRN_ID_MOV_SUBCTA
        --WHERE FFN_COD_MOV_ITGY IS NOT NULL AND FFN_POSICION_ITGY IS NOT NULL
        """, """
        INSERT INTO "TCGESPRO_MOVIMIENTO_PROFUTURO"(
            "FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES", "FCN_ID_TIPO_SUBCUENTA", "FTC_ORIGEN", "FTC_DESCRIPCION",
            "FTB_SWITCH"
        )
        VALUES (...)
        ON CONFLICT ("FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES") DO UPDATE 
        SET "FCN_ID_TIPO_SUBCUENTA" = EXCLUDED."FCN_ID_TIPO_SUBCUENTA", "FTC_ORIGEN" = EXCLUDED."FTC_ORIGEN",
            "FTC_DESCRIPCION" = EXCLUDED."FTC_DESCRIPCION", "FTB_SWITCH" = EXCLUDED."FTB_SWITCH"
        """, lambda i: [f":cod_mov_{i}", f":monpes_{i}", f":tipo_subcta_{i}", "'MIT'", f":description_{i}", "false"],
                       "TCGESPRO_MOVIMIENTO_PROFUTURO", upsert_id=lambda row: f"{str(row[0])}-{str(row[1])}")

        upsert_dataset(mit, postgres, """
        SELECT FFN_ID_CONCEPTO_MOV AS cod_mov,
               COALESCE(FFN_POSICION_ITGY, 0) AS monpes,
               S.FCN_ID_TIPO_SUBCTA AS tipo_subcta,
               M.FFC_DESC_ITGY AS description
        FROM CIERREN.TFCRXGRAL_CONFIG_MOV_ITGY M
        INNER JOIN TRAFOGRAL_MOV_SUBCTA S ON M.FRN_ID_MOV_SUBCTA = S.FRN_ID_MOV_SUBCTA
        --WHERE FFN_COD_MOV_ITGY IS NOT NULL AND FFN_POSICION_ITGY IS NOT NULL
        WHERE FFN_COD_MOV_ITGY IN (129,116,146,412,432,434,436,442,472,474,514,520,616,
                        646,724,727,905,915,917,923,925,926,927,932,936,942,
                        943,945,946,947,952,955,956,957,960,962,963,964,965,
                        966,972,973,974, 109,136,216,260,405,406,409,410,413,414,
                        416,420,421,423,424,426,430,433,440,441,443,444,
                        446,450,452,453,454,456,470,476,636,710,716,760,
                        809,914,922,924,930,933,934,944,954,921,213,106,110,
                        120,127,129,130,140,210,220,720,805,806,822,824, 841,
                        921
        )
        """, """
        INSERT INTO "TCGESPRO_MOVIMIENTO_PROFUTURO"(
            "FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES", "FCN_ID_TIPO_SUBCUENTA", "FTC_ORIGEN", "FTC_DESCRIPCION",
            "FTB_SWITCH"
        )
        VALUES (...)
        ON CONFLICT ("FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES") DO UPDATE 
        SET "FCN_ID_TIPO_SUBCUENTA" = EXCLUDED."FCN_ID_TIPO_SUBCUENTA", "FTC_ORIGEN" = EXCLUDED."FTC_ORIGEN",
            "FTC_DESCRIPCION" = EXCLUDED."FTC_DESCRIPCION", "FTB_SWITCH" = EXCLUDED."FTB_SWITCH"
        """, lambda i: [f":cod_mov_{i}", f":monpes_{i}", f":tipo_subcta_{i}", "'INTEGRITY'", f":description_{i}", "true"],
                       "TCGESPRO_MOVIMIENTO_PROFUTURO", upsert_id=lambda row: f"{str(row[0])}-{str(row[1])}")

        notify(
            postgres,
            f"Catálogos",
            phase,
            area,
            term=term_id,
            message=f"Se han ingestado los catálogos de forma exitosa para el periodo {time_period}",
            aprobar=False,
            descarga=False
        )
