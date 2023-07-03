from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_integrity_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
integrity_pool = get_integrity_pool("retiros")

phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, integrity_pool) as (postgres, integrity):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        truncate_table(postgres, "TEST_RETIROS", term=term_id)
        extract_dataset(integrity, postgres, """
        SELECT A.DET_NUMCUE, B.DET08_FECLIQ, A.DET_PERPAG,
               A.DET_FECTRA, A.DET_SECLOT, A.DET_SALBAS,
               A.DET_DIACOT, A.DET_AUSEN, A.DET_INCAPA,
               A.DET_RFCDEPEN, A.DET_NSSIMSS, A.DET_CURPAFORE,
               A.DET_FECPAG 
        FROM ISS_DISP_DET A, ISS_DISP_DET08 B
        WHERE A.DET_IDACEPTADO = 01
        AND A.DET_TIPREG = '02'
        AND (
            A.DET_RET > 0 OR
            A.DET_SAR > 0 OR
            A.DET_CVPAT > 0 OR
            A.DET_CVTRA > 0 OR
            A.DET_VIV92 > 0 OR
            A.DET_VIV08 > 0 OR
            A.DET_CUOSOC > 0 OR
            A.DET_SOLITRA > 0 OR
            A.DET_SOLIDEP > 0
        )
        AND  A.DET_FECTRA = B.DET08_FECTRA
        AND  A.DET_SECLOT = B.DET08_SECLOT
        AND  B.DET08_FECLIQ >= :start
        AND  B.DET08_FECLIQ <= :end
        GROUP BY A.DET_NUMCUE, B.DET08_FECLIQ, A.DET_PERPAG,
                 A.DET_FECTRA, A.DET_SECLOT, A.DET_SALBAS, 
                 A.DET_DIACOT, A.DET_AUSEN, A.DET_INCAPA,
                 A.DET_RFCDEPEN, A.DET_NSSIMSS, A.DET_CURPAFORE, 
                 A.DET_FECPAG
        """, "TEST_RETIROS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        })
