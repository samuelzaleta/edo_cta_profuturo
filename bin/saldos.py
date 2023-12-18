from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col, lit
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    print(term_id)
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción
        query = """
        SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               SH.FCN_ID_SIEFORE,
               SH.FCN_ID_TIPO_SUBCTA,
               SH.FTD_FEH_LIQUIDACION,
               :type AS FTC_TIPO_SALDO,
               MAX(VA.FCD_FEH_ACCION) AS FCD_FEH_ACCION,
               SUM(SH.FTN_DIA_ACCIONES) AS FTF_DIA_ACCIONES,
               SUM(ROUND(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION,2)) AS FTF_SALDO_DIA
        FROM cierren.thafogral_saldo_historico_v2 SH
        INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        INNER JOIN (
            SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
                   SHMAX.FCN_ID_SIEFORE,
                   SHMAX.FCN_ID_TIPO_SUBCTA,
                   MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
            FROM cierren.thafogral_saldo_historico_v2 SHMAX
            WHERE SHMAX.FTD_FEH_LIQUIDACION < (
                  SELECT MIN(SHMIN.FTD_FEH_LIQUIDACION)
                  FROM cierren.thafogral_saldo_historico_v2 SHMIN
                  WHERE SHMIN.FTD_FEH_LIQUIDACION > :date
              )
              -- AND SHMAX.FTN_NUM_CTA_INVDUAL 
              -- AND SHMAX.FCN_ID_TIPO_SUBCTA = 14
              -- AND SHMAX.FCN_ID_SIEFORE = 83
            GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
        ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                  AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                  AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
        INNER JOIN (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                   FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
            FROM cierren.TCAFOGRAL_VALOR_ACCION
            WHERE FCD_FEH_ACCION <= :date
        ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
            AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
            AND VA.ROW_NUM = 1
        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
        """

        truncate_table(postgres, "THHECHOS_SALDO_HISTORICO", term=term_id)
        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query,
            '"HECHOS"."THHECHOS_SALDO_HISTORICO"',
            term=term_id,
            params={"date": end_month, "type": "F"},
        )

        truncate_table(postgres, 'TTCALCUL_BONO', term=term_id)

        query_dias_rend_bono = """
                SELECT RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR AS FTD_FECHA_REDENCION_BONO,
                CAST(EXTRACT(DAY FROM RB.FTD_FEH_VALOR - TRUNC(:end)) AS INTEGER) AS DIAS_PLAZO_NATURALES
                FROM TTAFOGRAL_OP_INVDUAL RB
                WHERE (RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR) IN (
                                        SELECT FTN_NUM_CTA_INVDUAL,
                                               MAX(FTD_FEH_VALOR)
                                        FROM TTAFOGRAL_OP_INVDUAL
                                        GROUP BY FTN_NUM_CTA_INVDUAL)
                """

        query_vector = """
                SELECT FTN_PLAZO, FTN_FACTOR FROM TTAFOGRAL_VECTOR
                """

        query_saldos = """
                SELECT "FCN_CUENTA", "FTF_DIA_ACCIONES", "FTF_SALDO_DIA"
                FROM "HECHOS"."THHECHOS_SALDO_HISTORICO"
                WHERE "FCN_ID_PERIODO" = :term
                and "FCN_ID_TIPO_SUBCTA"  = 14
                """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_dias_rend_bono,
            "DIAS_REDENCION",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_vector,
            "VECTOR",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_postgres_spark,
            query_saldos,
            "SALDOS",
            params={"term": term_id}
        )

        df = spark.sql("""
                    SELECT 
                    S.FCN_CUENTA,
                    ROUND(S.FTF_DIA_ACCIONES,6) AS FTF_BON_NOM_ACC, 
                    ROUND(S.FTF_SALDO_DIA,2) AS FTF_BON_NOM_PES,
                    COALESCE(CASE 
                    WHEN S.FTF_DIA_ACCIONES > 0 THEN ROUND(S.FTF_DIA_ACCIONES * X.FTN_FACTOR,6) END, 0) FTF_BON_ACT_ACC,
                    COALESCE(CASE 
                    WHEN S.FTF_SALDO_DIA > 0 THEN ROUND(S.FTF_SALDO_DIA * X.FTN_FACTOR,2) END, 0) FTF_BON_ACT_PES,
                    FTD_FECHA_REDENCION_BONO AS FTD_FEC_RED_BONO,
                    FTN_FACTOR
                    FROM (SELECT 
                            DR.FTN_NUM_CTA_INVDUAL,DR.FTD_FECHA_REDENCION_BONO,
                            DR.DIAS_PLAZO_NATURALES, VT.FTN_FACTOR
                            FROM DIAS_REDENCION DR
                                INNER JOIN VECTOR VT
                                ON VT.FTN_PLAZO = DR.DIAS_PLAZO_NATURALES
                        ) X
                            INNER JOIN SALDOS S 
                            ON X.FTN_NUM_CTA_INVDUAL = S.FCN_CUENTA
                """)

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))

        _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TTCALCUL_BONO"')

        # Cifras de control
        report1 = html_reporter.generate(
            postgres,
            """
            SELECT
                I."FTC_GENERACION" AS GENERACION,
                I."FTC_VIGENCIA" AS VIGENCIA,
                I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                I."FTC_ORIGEN" AS ORIGEN,
                TS."FCC_VALOR" AS TIPO_SUBCUENTA,
                S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2) AS SALDO_INICIAL_PESOS,
                TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END):: NUMERIC, 2)AS SALDO_FINAL_PESOS,
                --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_INICIAL_ACCIONES,
                TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_DIA_ACCIONES" ELSE 0 END):: NUMERIC, 6) AS SALDO_FINAL_ACCIONES
            FROM "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON SH."FCN_CUENTA" = I."FCN_CUENTA"
            INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
            INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE SH."FCN_ID_PERIODO" = :term and I."FCN_ID_PERIODO" = :term
            GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA",I."FTC_GENERACION" , I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
            """,
            ["Generación", "Vigencia", "tipo_cliente", "Origen", "Sub cuenta", "SIEFORE"],
            ["Saldo final en pesos", "Saldo final en acciones"],
            params={"term": term_id},
        )

        report2 = html_reporter.generate(
            postgres,
            """
            SELECT TS."FCC_VALOR" AS TIPO_SUBCUENTA,
                   S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                   --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2) AS SALDO_INICIAL_PESOS,
                   TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2)AS SALDO_FINAL_PESOS,
                   --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_INICIAL_ACCIONES,
                   TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_FINAL_ACCIONES
            FROM "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
                INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
                INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE "FCN_ID_PERIODO" = :term
            GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA"
            """,
            ["TIPO_SUBCUENTA", "SIEFORE"],
            ["Saldo final en pesos", "Saldo final en acciones"],
            params={"term": term_id},
        )

        notify(
            postgres,
            f"Saldos",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para saldos exitosamente para el periodo",
            details=report1,
        )
        notify(
            postgres,
            f"Saldos",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para saldos exitosamente para el periodo",
            details=report2,
        )
