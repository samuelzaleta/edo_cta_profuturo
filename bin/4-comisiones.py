from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter
from datetime import date

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = 2

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    terms = extract_terms(postgres, phase)

    for term in terms:
        term_id = term["id"]
        start_month = term["start_month"]
        end_month = term["end_month"]

        with register_time(postgres, phase, term=term_id):
            # Extracción
            truncate_table(postgres, 'tthechos_comisiones', term=term_id)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, FTN_ID_MOV AS FCN_ID_MOV, 
                   FTC_FOLIO, FCN_ID_SIEFORE, FTF_MONTO_ACCIONES, FTD_FEH_LIQUIDACION, 
                   FTF_MONTO_PESOS
            FROM TTAFOGRAL_MOV_CMS
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "TTHECHOS_COMISION", term=term_id, params={"start": date(2022, 9, 1), "end": date(2022, 9, 30)})

            # Cifras de control
            report = html_reporter.generate(postgres, """
            SELECT fto_indicadores->>'34' AS generacion,
                   fto_indicadores->>'21' AS vigencia,
                   CASE
                       WHEN fto_indicadores->>'3' = 'Asignado' THEN 'Asignado'
                       WHEN fto_indicadores->>'4' = 'Pensionado' THEN 'Pensionado'
                       WHEN fto_indicadores->>'3' = 'Afiliado' THEN 'Afiliado'
                   END AS tipo_formato,
                   fto_indicadores->>'33' AS tipo_cliente,
                   s.fcc_valor AS siefore,
                   COUNT(*) AS clientes,
                   SUM(ftf_monto_pesos) AS comisiones
            FROM tthechos_comisiones mc
                LEFT JOIN catalogo_siefores s ON mc.fcn_id_siefore = s.fcn_id_cat_catalogo
                INNER JOIN tcdatmae_clientes c ON mc.fcn_cuenta = c.ftn_cuenta
            GROUP BY fto_indicadores->>'34',
                     fto_indicadores->>'21',
                     CASE
                         WHEN fto_indicadores->>'3' = 'Asignado' THEN 'Asignado'
                         WHEN fto_indicadores->>'4' = 'Pensionado' THEN 'Pensionado'
                         WHEN fto_indicadores->>'3' = 'Afiliado' THEN 'Afiliado'
                     END,
                     fto_indicadores->>'33',
                     s.fcc_valor
            """, ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"], ["Registros", "Comisiones"])

            notify(
                postgres,
                "Cifras de control Comisiones generadas",
                "Se han generado las cifras de control para comisiones exitosamente",
                report,
                term=term_id,
                control=True,
            )
