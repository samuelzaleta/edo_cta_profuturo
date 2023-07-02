from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_integrity_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
integrity_pool = get_integrity_pool()

phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, integrity_pool) as (postgres, integrity):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
         extract_dataset(integrity, postgres, """
         SELECT CSIE1_NUMCUE AS FCN_CUENTA, 
                CSIE1_CODMOV AS FCN_ID_TIPO_MOVIMIENTO,
                CVE_SIEFORE AS FCN_ID_SIEFORE,
                CASE 
                    WHEN CSIE1_MONPES_1 > 0 THEN CSIE1_MONPES_1
                    WHEN CSIE1_MONPES_3 > 0 THEN CSIE1_MONPES_3
                    WHEN CSIE1_MONPES_5 > 0 THEN CSIE1_MONPES_5
                    WHEN CSIE1_MONPES_8 > 0 THEN CSIE1_MONPES_8
                    WHEN CSIE1_MONPES_9 > 0 THEN CSIE1_MONPES_9
                END AS FTF_MONTO_PESOS,
                'I' AS FTC_BD_ORIGEN--,
                -- CSIE1_FECCON, 
                -- CVE_SERVICIO, CSIE1_FECHA_2,
                -- CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL,
                -- CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
         FROM MOV_GOBIERNO
         WHERE CSIE1_FECCON >= :start
           AND CSIE1_FECCON <= :end
           AND CSIE1_CODMOV IN (
               106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
               426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
               453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
           )
         LIMIT 10
         """, 'TEST_MOVIMIENTOS', term=term_id, params={
             'start': start_month.strftime("%Y%m%d"),
             'end': end_month.strftime("%Y%m%d"),
         })
