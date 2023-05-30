from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = 5

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    terms = extract_terms(postgres, phase)

    for term in terms:
        term_id = term["id"]
        start_month = term["start_month"]
        end_month = term["end_month"]

        with register_time(postgres, phase, term=term_id):
            # TODO SUA Extraction
            pass
