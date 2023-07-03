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
        truncate_table(postgres, 'TTHECHOS_SUA', term=term_id)
        extract_dataset(mit, postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, FNC_RFC_PATRON,
               FNN_ULTIMO_SALARIO_INT_PER, FND_FECHA_VALOR_RCV, FNN_SECLOT,
               FND_FECTRA, FND_FECHA_PAGO, FTC_FOLIO, FNN_ID_REFERENCIA AS FTN_REFERENCIA,
               FNN_DIAS_COTZDOS_BIMESTRE, FNN_DIAS_INCAP_BIMESTRE, FNN_DIAS_AUSENT_BIMESTRE
        FROM CIERREN.TNAFORECA_SUA
        WHERE FTC_FOLIO IN ('202210111051305573', '202210251250485875', '202210261121355883', '202210271354305925', '202210271648515929', '202210111259025579', '202210181148525671', '202210042245565223', '202210111046525572', '202210211301285689', '202210042253335225', '202210042249245224')
        """, "TTHECHOS_SUA", term=term_id, params={"start": start_month, "end": end_month}, limit=1000)

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
                   COUNT(DISTINCT c."FTN_CUENTA") AS clientes,
                   SUM(m."FTN_REFERENCIA") AS REFERENCIA
            FROM "TTHECHOS_SUA" m
                INNER JOIN "TCDATMAE_CLIENTE" c on m."FCN_CUENTA" = c."FTN_CUENTA"
                INNER JOIN "TCHECHOS_CLIENTE" i ON c."FTN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            GROUP BY "FTO_INDICADORES"->>'34',
                     "FTO_INDICADORES"->>'21',
                     CASE
                         WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                         WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                         WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                     END,
                     "FTO_INDICADORES"->>'33'
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"],
            ["Registros", "Comisiones"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control sua generadas",
            "Se han generado las cifras de control para comisiones exitosamente",
            report,
            term=term_id,
            control=True,
        )
