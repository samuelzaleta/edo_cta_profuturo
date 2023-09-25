from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_integrity_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
integrity_pool = get_integrity_pool("historia")

phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, integrity_pool) as (postgres, integrity):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        truncate_table(postgres, "TEST_HISTORIA", term=term_id)
        extract_dataset(integrity, postgres, """
        SELECT DISP_NUMCUE, DISP_NSSTRA, DISP_FECTRA, DISP_SECLOT, DISP_PERPAG,
               DISP_CORREL, DISP_TIPREG, DISP_SALBAS, DISP_DIACOT, DISP_AUSENT,
               DISP_INCAPA, DISP_FECLIQ_RCV, DISP_NSSEMP, DISP_RFCEMP, DISP_FOLSUA
        FROM SUA
        WHERE DISP_FECTRA >= :start
          AND DISP_FECTRA <= :end
        """, "TEST_HISTORIA", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        })
