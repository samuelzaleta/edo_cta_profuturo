from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import upsert_dataset, extract_terms, _get_spark_session
from pyspark.sql.functions import col
import sys
from datetime import datetime

postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
user = int(sys.argv[3])

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, area, user, term_id):

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
        """, lambda i: [f":id_{i}", f":regime_id_{i}", f":subacc_cat_id_{i}", f":description_{i}"],
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
                       "TCGESPRO_MOVIMIENTO_PROFUTURO")

        upsert_dataset(mit, postgres, """
        SELECT FFN_ID_CONCEPTO_MOV AS cod_mov,
               COALESCE(FFN_POSICION_ITGY, 0) AS monpes,
               S.FCN_ID_TIPO_SUBCTA AS tipo_subcta,
               M.FFC_DESC_ITGY AS description
        FROM CIERREN.TFCRXGRAL_CONFIG_MOV_ITGY M
        INNER JOIN TRAFOGRAL_MOV_SUBCTA S ON M.FRN_ID_MOV_SUBCTA = S.FRN_ID_MOV_SUBCTA
        --WHERE FFN_COD_MOV_ITGY IS NOT NULL AND FFN_POSICION_ITGY IS NOT NULL
        WHERE FFN_COD_MOV_ITGY IN (
            106, 109, 129, 210, 260, 405, 406, 410, 412, 413, 414, 416, 420, 421, 423, 424, 426, 430, 433, 436, 440, 
            440, 441, 442, 443, 444, 446, 450, 452, 453, 454, 456, 470, 472, 474, 476, 610, 630, 710, 760, 
            805, 806, 841
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
        """, lambda i: [f":cod_mov_{i}", f":monpes_{i}", f":tipo_subcta_{i}", "'INTEGRITY'", f":description_{i}",
                        "true"], "TCGESPRO_MOVIMIENTO_PROFUTURO")

        """
        tables = ["TCDATMAE_SIEFORE", "TCDATMAE_TIPO_SUBCUENTA", "TCGESPRO_MOVIMIENTO_PROFUTURO"]


        def print_html_tables(tables):
            for table in tables:
                df = spark.sql(f"select * from {table}")

                pandas_df = df.select(*[col(c).cast("string") for c in df.columns]).toPandas()

                # Obtener el valor máximo de id
                max_id = spark.sql(f"SELECT MAX(id) FROM {table}").collect()[0][0]

                pandas_df['id'] = pandas_df['id'].astype(int)
                # Dividir el resultado en tablas HTML de 50 en 50
                batch_size = 100
                for start in range(0, max_id, batch_size):
                    end = start + batch_size
                    batch_pandas_df = pandas_df[(pandas_df['id'] >= start) & (pandas_df['id'] < end)]
                    batch_html_table = batch_pandas_df.to_html(index=False)

                    # Enviar notificación con la tabla HTML de este lote
                    notify(
                        postgres,
                        f"Cifras de control {table} generadas - Parte {start}-{end - 1}",
                        f"Se han generado las cifras de control para {table} exitosamente",
                        batch_html_table,
                        term=term_id,
                        control=True,
                    )


        print_html_tables(tables)
"""
        notify(
            postgres,
            f"Catálogos ingestados - {datetime.now()}",
            f"Se han ingestado los catálogos de forma exitosa para el periodo {time_period}",
            term=term_id,
            area=area,
            fase=phase,
            control=False,
            control_validadas=True
        )
