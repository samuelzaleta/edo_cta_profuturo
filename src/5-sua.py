from common import MemoryLogger, HtmlReporter, get_postgres_pool, get_mit_pool, get_buc_pool, truncate_table, extract_terms, extract_dataset, notify


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
