from common import MemoryLogger, HtmlReporter, get_postgres_pool, get_mit_pool, get_buc_pool, truncate_table, extract_terms, extract_dataset, notify
from datetime import date


app_logger = MemoryLogger()
html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
buc_pool = get_buc_pool()

with postgres_pool.begin() as postgres:
    terms = extract_terms(postgres)

    with mit_pool.begin() as mit:
        for term in terms:
            term_id = term["id"]
            start_month = term["start_month"]
            end_month = term["end_month"]

            # Extracción
            truncate_table(postgres, 'tthechos_comisiones', term_id)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, FTN_ID_MOV AS FCN_ID_MOV, 
                   FTC_FOLIO, FCN_ID_SIEFORE, FTF_MONTO_ACCIONES, FTD_FEH_LIQUIDACION, 
                   FTF_MONTO_PESOS
            FROM TTAFOGRAL_MOV_CMS
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_comisiones", term_id, phase=4, params={"start": date(2022, 9, 1), "end": date(2022, 9, 30)})

            # Cifras de control
            report = html_reporter.generate(postgres, """
            SELECT ftc_generacion,
                   ftc_vigencia,
                   CASE
                       WHEN ftc_origen = 'Asignado' THEN 'Asignado'
                       WHEN ftb_pension = true AND ftb_bono = true THEN 'Pensionado con Bono'
                       WHEN ftb_pension = true THEN 'Pensionado'
                       WHEN ftc_origen = 'Afiliado' AND ftb_bono = true THEN 'Afiliado con Bono'
                       WHEN ftc_origen = 'Afiliado' THEN 'Afiliado'
                   END AS tipo_formato,
                   ftc_tipo_cliente,
                   s.fcc_valor AS siefore,
                   COUNT(*) AS clientes,
                   SUM(ftf_monto_pesos) AS comisiones
            FROM tthechos_comisiones mc
                LEFT JOIN catalogo_siefores s ON mc.fcn_id_siefore = s.fcn_id_cat_catalogo
                INNER JOIN tcdatmae_clientes c ON mc.fcn_cuenta = c.ftn_cuenta
            GROUP BY ftc_generacion,
                     ftc_vigencia,
                     CASE
                         WHEN ftc_origen = 'Asignado' THEN 'Asignado'
                         WHEN ftb_pension = true AND ftb_bono = true THEN 'Pensionado con Bono'
                         WHEN ftb_pension = true THEN 'Pensionado'
                         WHEN ftc_origen = 'Afiliado' AND ftb_bono = true THEN 'Afiliado con Bono'
                         WHEN ftc_origen = 'Afiliado' THEN 'Afiliado'
                     END,
                     ftc_tipo_cliente,
                     s.fcc_valor
            """, ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"], ["Registros", "Comisiones"])

            notify(
                postgres,
                term_id,
                "Cifras de control Comisiones generadas",
                "Se han generado las cifras de control para comisiones exitosamente",
                report,
                control=True,
            )