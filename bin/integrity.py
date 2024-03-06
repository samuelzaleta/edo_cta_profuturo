from profuturo.common import notify, register_time, define_extraction, truncate_table
from profuturo.database import get_postgres_pool, get_integrity_pool,  configure_postgres_spark
from profuturo.extraction import _get_spark_session,extract_terms, extract_dataset, extract_dataset_return, _write_spark_dataframe
from profuturo.reporters import HtmlReporter
from pandas import DataFrame
from sqlalchemy import text
import numpy as np
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
integrity_pool = get_integrity_pool("cierren")

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, integrity_pool) as (postgres, integrity):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    #truncate_table(postgres, 'TTHECHOS_MOV_ITGY_GOBIERNO', term=term_id)
    extract_dataset(integrity, postgres, """
            SELECT CSIE1_NUMCUE, 
                   CSIE1_CODMOV,
                   CSIE1_FECCON,
                   CVE_SIEFORE,
                   CVE_SERVICIO, CSIE1_MONPES_1, CSIE1_MONPES_3, 
                   CSIE1_MONPES_5, CSIE1_MONPES_8, CSIE1_MONPES_9, 
                   CSIE1_NSSEMP, CSIE1_FECHA_2,
                   CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL,
                   CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
            FROM MOV_GOBIERNO
            WHERE CSIE1_FECCON >= :start
              AND CSIE1_FECCON <= :end
              AND CSIE1_CODMOV  IN (
                  106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
                  426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
                  453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
              )
            """, "TTHECHOS_MOV_ITGY_GOBIERNO", term=term_id, params={
        "start": start_month.strftime("%Y%m%d"),
        "end": end_month.strftime("%Y%m%d"),
    })

    #truncate_table(postgres, 'TTHECHOS_MOV_ITGY_RCV', term=term_id)
    extract_dataset(integrity, postgres, """
            SELECT CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE,
                   CVE_SERVICIO, CSIE1_MONPES_1,
                   CSIE1_MONPES_2, CSIE1_MONPES_3, CSIE1_MONPES_4,
                   CSIE1_MONPES_7, CSIE1_MONPES_8,
                   CSIE1_NSSEMP, CSIE1_FECHA_2,
                   CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL,
                   CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
            FROM MOV_RCV
            WHERE CSIE1_FECCON >= :start
              AND CSIE1_FECCON <= :end
              AND CSIE1_CODMOV IN (
                  106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
                  426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
                  453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
              )
            """, "TTHECHOS_MOV_ITGY_RCV", term=term_id, params={
        "start": start_month.strftime("%Y%m%d"),
        "end": end_month.strftime("%Y%m%d"),
    })

