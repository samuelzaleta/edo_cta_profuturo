from profuturo.common import truncate_table, notify, register_time
from profuturo.database import get_postgres_pool, get_mit_pool, use_pools
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = 5

with use_pools(phase, postgres_pool, mit_pool) as (postgres, mit):
    with register_time(postgres, phase):
        terms = extract_terms(postgres, phase)

        for term in terms:
            term_id = term["id"]
            start_month = term["start_month"]
            end_month = term["end_month"]