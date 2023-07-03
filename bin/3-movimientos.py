from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        # Extracción
        truncate_table(postgres, 'TTHECHOS_MOVIMIENTO', term=term_id)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FCN_ID_TIPO_SUBCTA,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTF_MONTO_PESOS,
               FTD_FEH_LIQUIDACION,
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_AVOL
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FCN_ID_TIPO_SUBCTA,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTF_MONTO_PESOS,
               FTD_FEH_LIQUIDACION,
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_BONO
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FCN_ID_TIPO_SUBCTA,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTF_MONTO_PESOS,
               FTD_FEH_LIQUIDACION,
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_COMP
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FCN_ID_TIPO_SUBCTA,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTF_MONTO_PESOS,
               FTD_FEH_LIQUIDACION,
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_GOB
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FCN_ID_TIPO_SUBCTA,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTF_MONTO_PESOS,
               FTD_FEH_LIQUIDACION,
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_RCV
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FCN_ID_TIPO_SUBCTA,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTF_MONTO_PESOS,
               FTD_FEH_LIQUIDACION,
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_SAR
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FCN_ID_TIPO_SUBCTA,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTF_MONTO_PESOS,
               FTD_FEH_LIQUIDACION,
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_VIV
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)

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
                   END AS tipo_afiliado,
                   "FTO_INDICADORES"->>'33' AS tipo_cliente,
                   mc."FTC_DESCRIPCION",
                   COUNT(DISTINCT c."FTN_CUENTA") AS clientes,
                   SUM(m."FTF_MONTO_PESOS") AS importe
            FROM "TTHECHOS_MOVIMIENTO" m
                INNER JOIN "TTGESPRO_MOV_PROFUTURO_CONSAR" pc ON m."FCN_ID_TIPO_MOVIMIENTO" = pc."FCN_ID_MOVIMIENTO_PROFUTURO"
                INNER JOIN "TCDATMAE_MOVIMIENTO_CONSAR" mc on pc."FCN_ID_MOVIMIENTO_CONSAR" = mc."FTN_ID_MOVIMIENTO_CONSAR"
                INNER JOIN "TCDATMAE_CLIENTE" c on m."FCN_CUENTA" = c."FTN_CUENTA"
                INNER JOIN "TCHECHOS_CLIENTE" i ON c."FTN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            GROUP BY "FTO_INDICADORES"->>'34',
                     "FTO_INDICADORES"->>'21',
                     CASE
                         WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                         WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                         WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                     END,
                     "FTO_INDICADORES"->>'33',
                     mc."FTC_DESCRIPCION"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"],
            ["Registros", "Comisiones"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control Comisiones generadas",
            "Se han generado las cifras de control para comisiones exitosamente",
            report,
            term=term_id,
            control=True,
        )
