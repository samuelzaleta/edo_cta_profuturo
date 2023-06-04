from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = 3

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    terms = extract_terms(postgres, phase)

    for term in terms:
        term_id = term["id"]
        start_month = term["start_month"]
        end_month = term["end_month"]

        with register_time(postgres, phase, term=term_id):
            # Extracción
            truncate_table(postgres, 'TTHECHOS_MOVIMIENTO', term=term_id)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_AVOL
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_BONO
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_COMP
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_GOB
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_RCV
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_SAR
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_VIV
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_MOVIMIENTO", term=term_id, params={"start": start_month, "end": end_month}, limit=100)

            # Cifras de control
            report = html_reporter.generate(
                postgres,
                """
                SELECT fto_indicadores->>'34' AS generacion,
                       fto_indicadores->>'21' AS vigencia,
                       CASE
                           WHEN fto_indicadores->>'3' = 'Asignado' THEN 'Asignado'
                           WHEN fto_indicadores->>'4' = 'Pensionado' THEN 'Pensionado'
                           WHEN fto_indicadores->>'3' = 'Afiliado' THEN 'Afiliado'
                       END AS tipo_formato,
                       fto_indicadores->>'33' AS tipo_cliente,
                       mc.ftc_descripcion,
                       COUNT(DISTINCT c.ftn_cuenta) AS clientes,
                       SUM(m.ftf_monto_pesos) AS importe
                FROM tthechos_movimientos m
                    INNER JOIN ttgespro_mov_profuturo_consar pc ON m.fcn_id_concepto_mov = pc.fcc_id_movimiento_profuturo
                    INNER JOIN tcdatmae_movimientos_consar mc on pc.fcn_id_movimiento_consar = mc.ftn_id_movimiento_consar
                    INNER JOIN tcdatmae_clientes c on m.fcn_cuenta = c.ftn_cuenta
                GROUP BY fto_indicadores->>'34',
                         fto_indicadores->>'21',
                         CASE
                             WHEN fto_indicadores->>'3' = 'Asignado' THEN 'Asignado'
                             WHEN fto_indicadores->>'4' = 'Pensionado' THEN 'Pensionado'
                             WHEN fto_indicadores->>'3' = 'Afiliado' THEN 'Afiliado'
                         END,
                         fto_indicadores->>'33',
                         mc.ftc_descripcion
                """,
                ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"],
                ["Registros", "Comisiones"],
            )

            notify(
                postgres,
                "Cifras de control Comisiones generadas",
                "Se han generado las cifras de control para comisiones exitosamente",
                report,
                term=term_id,
                control=True,
            )
