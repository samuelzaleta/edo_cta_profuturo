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
            query = """
            WITH ultima_mod AS (
                SELECT FTN_NUM_CTA_INVDUAL,
                       FCN_ID_TIPO_SUBCTA,
                       FCN_ID_SIEFORE,
                       TRUNC(FTD_FEH_LIQUIDACION, 'MM') AS START_OF_MONTH
                FROM cierren.thafogral_saldo_historico_v2
                WHERE FTN_NUM_CTA_INVDUAL > 0
                  AND FTD_FEH_LIQUIDACION <= :date
                GROUP BY FTN_NUM_CTA_INVDUAL,
                         FCN_ID_TIPO_SUBCTA,
                         FCN_ID_SIEFORE,
                         TRUNC(FTD_FEH_LIQUIDACION, 'MM')
            ), valor_accion AS (
                SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                       FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
                FROM TCAFOGRAL_VALOR_ACCION
                WHERE FCD_FEH_ACCION <= :date
            )
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
            INNER JOIN ultima_mod UM ON SH.FTN_NUM_CTA_INVDUAL = UM.FTN_NUM_CTA_INVDUAL
                                    AND SH.FCN_ID_TIPO_SUBCTA = UM.FCN_ID_TIPO_SUBCTA
                                    AND SH.FCN_ID_SIEFORE = UM.FCN_ID_SIEFORE
                                    AND SH.FTD_FEH_LIQUIDACION = UM.START_OF_MONTH
            INNER JOIN valor_accion VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
                                      AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
                                      AND VA.ROW_NUM = 1
            GROUP BY SH.FTN_NUM_CTA_INVDUAL,
                     SH.FCN_ID_SIEFORE,
                     SH.FCN_ID_TIPO_SUBCTA,
                     SH.FTD_FEH_LIQUIDACION
            """

            truncate_table(postgres, "THHECHOS_SALDO_HISTORICO", term=term_id)
            extract_dataset(mit, postgres, query, "THHECHOS_SALDO_HISTORICO", term=term_id, params={"date": start_month, "type": "I"})
            extract_dataset(mit, postgres, query, "THHECHOS_SALDO_HISTORICO", term=term_id, params={"date": end_month, "type": "F"})

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
                       ts.fcc_valor AS subcuenta,
                       s.fcc_valor AS siefore,
                       COUNT(*) AS clientes,
                       SUM(CASE sh.ftc_tipo_saldo WHEN 'I' THEN sh.ftf_saldo_dia ELSE 0 END) AS saldo_inicial,
                       SUM(CASE sh.ftc_tipo_saldo WHEN 'F' THEN sh.ftf_saldo_dia ELSE 0 END) AS saldo_final
                FROM thhechos_saldo_historico sh
                    INNER JOIN tcdatmae_tipos_subcuenta ts ON sh.fcn_id_tipo_subcta = ts.ftn_id_tipo_subcta
                    INNER JOIN catalogo_siefores s ON sh.fcn_id_siefore = s.fcn_id_cat_catalogo
                    INNER JOIN tcdatmae_clientes c ON sh.fcn_cuenta = c.ftn_cuenta
                GROUP BY fto_indicadores->>'34',
                         fto_indicadores->>'21',
                         CASE
                             WHEN fto_indicadores->>'3' = 'Asignado' THEN 'Asignado'
                             WHEN fto_indicadores->>'4' = 'Pensionado' THEN 'Pensionado'
                             WHEN fto_indicadores->>'3' = 'Afiliado' THEN 'Afiliado'
                         END,
                         fto_indicadores->>'33',
                         ts.fcc_valor,
                         s.fcc_valor
                """,
                ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "Sub Cuenta", "SIEFORE"],
                ["Clientes", "Saldo inicial", "Saldo final"],
            )

            notify(
                postgres,
                "Cifras de control Saldos generadas",
                "Se han generado las cifras de control para saldos exitosamente",
                report,
                term=term_id,
                control=True,
            )
