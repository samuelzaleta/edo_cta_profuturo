from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool, get_postgres_url
from profuturo.extraction import extract_terms, extract_dataset_polars
from profuturo.reporters import HtmlReporter

import sys

#html_reporter = HtmlReporter()
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
        truncate_table(postgres, "TTEDOCTA_AFILIADO", term=term_id)
        extract_dataset_polars(get_postgres_url(), postgres, """
        SELECT ota.*
        FROM "RESULTADOS".obtener_ttedocta_afiliado() AS ota
        """, "TTEDOCTA_AFILIADO", term=term_id)
