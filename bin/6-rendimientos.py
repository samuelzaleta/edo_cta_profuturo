from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_terms, extract_dataset_polars
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
        truncate_table(postgres, "TTCALCUL_RENDIMIENTO", term=term_id)
        extract_dataset_polars(postgres, postgres, """
        SELECT cmr.*
        FROM "HECHOS".calcular_movimientos_rendimientos(:term, :start, :end) AS cmr
        """, "TTCALCUL_RENDIMIENTO", term=term_id, params={"term": term_id, "start": start_month, "end": end_month})

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
                   ts."FCC_VALOR" AS tipo_subcuenta,
                   COUNT(DISTINCT c."FTN_CUENTA") AS clientes,
                   SUM(m."FTF_RENDIMIENTO_CALCULADO") AS rendimiento
            FROM "TTCALCUL_RENDIMIENTO" m
                INNER JOIN "TCDATMAE_TIPO_SUBCUENTA" ts ON m."FCN_ID_TIPO_SUBCTA" = m."FCN_ID_TIPO_SUBCTA"
                INNER JOIN "TCDATMAE_CLIENTE" c on m."FCN_CUENTA" = c."FTN_CUENTA"
                INNER JOIN "TCHECHOS_CLIENTE" i ON c."FTN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            WHERE m."FCN_ID_PERIODO" = :term
            GROUP BY "FTO_INDICADORES"->>'34',
                     "FTO_INDICADORES"->>'21',
                     CASE
                         WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                         WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                         WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                     END,
                     "FTO_INDICADORES"->>'33',
                     ts."FCC_VALOR"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "Tipo Subcuenta"],
            ["Registros", "Rendimiento"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control Rendimiento generadas",
            "Se han generado las cifras de control para rendimientos exitosamente",
            report,
            term=term_id,
            control=True,
        )
