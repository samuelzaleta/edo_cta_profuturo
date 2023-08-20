from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, extract_dataset_spark
from profuturo.reporters import HtmlReporter
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        # Extracción
        query = """
        SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               SH.FCN_ID_SIEFORE,
               SH.FCN_ID_TIPO_SUBCTA,
               SH.FTD_FEH_LIQUIDACION,
               :type AS FTC_TIPO_SALDO,
               MAX(VA.FCD_FEH_ACCION) AS FCD_FEH_ACCION,
               SUM(SH.FTN_DIA_ACCIONES) AS FTN_DIA_ACCIONES,
               SUM(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION) AS FTF_SALDO_DIA
        FROM cierren.thafogral_saldo_historico_v2 SH
        INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        INNER JOIN (
            SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
                   SHMAX.FCN_ID_SIEFORE,
                   SHMAX.FCN_ID_TIPO_SUBCTA,
                   MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
            FROM cierren.thafogral_saldo_historico_v2 SHMAX
            WHERE SHMAX.FTD_FEH_LIQUIDACION <= :date
              -- AND SHMAX.FTN_NUM_CTA_INVDUAL = 10044531
              -- AND SHMAX.FCN_ID_TIPO_SUBCTA = 22
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
            params={"date": start_month, "type": "I"},
        )
        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query,
            '"HECHOS"."THHECHOS_SALDO_HISTORICO"',
            term=term_id,
            params={"date": end_month, "type": "F"},
        )

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
                ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2) AS SALDO_INICIAL_PESOS,
                ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2)AS SALDO_FINAL_PESOS,
                ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_INICIAL_ACCIONES,
                ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_FINAL_ACCIONES
            FROM "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON SH."FCN_CUENTA" = I."FCN_CUENTA"
            INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
            INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE SH."FCN_ID_PERIODO" = :term and I."FCN_ID_PERIODO" = :term
            GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA",I."FTC_GENERACION" , I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
            """,
            ["Generación", "Vigencia", "tipo_cliente", "Origen", "Sub cuenta", "SIEFORE"],
            ["Saldo inicial en pesos", "Saldo final en pesos", "Saldo inicial en acciones", "Saldo final en acciones"],
            params={"term": term_id},
        )

        report2 = html_reporter.generate(
            postgres,
            """
            SELECT TS."FCC_VALOR" AS TIPO_SUBCUENTA,
                   S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                   ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2) AS SALDO_INICIAL_PESOS,
                   ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2)AS SALDO_FINAL_PESOS,
                   ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_INICIAL_ACCIONES,
                   ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_FINAL_ACCIONES
            FROM "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
                INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
                INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE "FCN_ID_PERIODO" = :term
            GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA"
            """,
            ["SIEFORE"],
            ["Saldo inicial en pesos", "Saldo final en pesos", "Saldo inicial en acciones", "Saldo final en acciones"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control Saldos generadas 1 de 2",
            "Se han generado las cifras de control para saldos exitosamente",
            report1,
            term=term_id,
            control=True,
        )

        notify(
            postgres,
            "Cifras de control Saldos generadas 2 de 2",
            "Se han generado las cifras de control para saldos exitosamente",
            report2,
            term=term_id,
            control=True,
        )