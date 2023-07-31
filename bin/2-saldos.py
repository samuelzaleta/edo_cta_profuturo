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
        INNER JOIN TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
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
            FROM TCAFOGRAL_VALOR_ACCION
            WHERE FCD_FEH_ACCION <= :date
        ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
            AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
            AND VA.ROW_NUM = 1
        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
        """

        truncate_table(postgres, 'THHECHOS_SALDO_HISTORICO', term=term_id)
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, query, 'THHECHOS_SALDO_HISTORICO', term=term_id, params={"date": start_month, "type": "I"})
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, query, 'THHECHOS_SALDO_HISTORICO', term=term_id, params={"date": end_month, "type": "F"})

        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
            SELECT "FTO_INDICADORES"->>'34' AS generacion,
                   "FTO_INDICADORES"->>'21' AS vigencia,
                   CASE
                       WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                       WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                       WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                   END AS tipo_afiliación,
                   "FTO_INDICADORES"->>'33' AS tipo_cliente,
                   ts."FCC_VALOR" AS subcuenta,
                   s."FTC_DESCRIPCION" AS siefore,
                   COUNT(*) AS clientes,
                   SUM(CASE sh."FTC_TIPO_SALDO" WHEN 'I' THEN sh."FTF_SALDO_DIA" ELSE 0 END) AS saldo_inicial,
                   SUM(CASE sh."FTC_TIPO_SALDO" WHEN 'F' THEN sh."FTF_SALDO_DIA" ELSE 0 END) AS saldo_final
            FROM "THHECHOS_SALDO_HISTORICO" sh
                INNER JOIN "TCDATMAE_TIPO_SUBCUENTA" ts ON sh."FCN_ID_TIPO_SUBCTA" = ts."FTN_ID_TIPO_SUBCTA"
                INNER JOIN "TCDATMAE_SIEFORE" s ON sh."FCN_ID_SIEFORE" = s."FTN_ID_SIEFORE"
                INNER JOIN "TCDATMAE_CLIENTE" c ON sh."FCN_CUENTA" = c."FTN_CUENTA"
                INNER JOIN "TCHECHOS_CLIENTE" i ON c."FTN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            GROUP BY "FTO_INDICADORES"->>'34',
                     "FTO_INDICADORES"->>'21',
                     CASE
                         WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                         WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                         WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                     END,
                     "FTO_INDICADORES"->>'33',
                     ts."FCC_VALOR",
                     s."FTC_DESCRIPCION"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "Sub Cuenta", "SIEFORE"],
            ["Clientes", "Saldo inicial", "Saldo final"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control Saldos generadas",
            "Se han generado las cifras de control para saldos exitosamente",
            report,
            term=term_id,
            control=True,
        )
