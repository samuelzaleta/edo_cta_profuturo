from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter

import sys
import json

#html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])
SALD_AVES = json.loads(sys.argv[3])
SDOR_AVOL = json.loads(sys.argv[4])
SDOR_ACR = json.loads(sys.argv[5])
SDOR_ALP = json.loads(sys.argv[6])
SDOR_AAS = json.loads(sys.argv[7])


with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        # Extracción
        truncate_table(postgres, "TTEDOCTA_GENERACION_AFORE", term=term_id)
        extract_dataset(postgres, postgres, """
        SELECT
            ctga.*
        FROM "RESULTADOS".calcular_ttedocta_generacion_afore(:term, :start, :end, :SALD_AVES, :SDOR_AVOL, :SDOR_ACR, :SDOR_ALP, :SDOR_AAS) AS ctga
        """, "TTEDOCTA_GENERACION_AFORE", term=term_id, params={"term": term_id, "start": start_month, "end": end_month, "SALD_AVES": SALD_AVES,
        "SDOR_AVOL": SDOR_AVOL, "SDOR_ACR": SDOR_ACR, "SDOR_ALP": SDOR_ALP, "SDOR_AAS": SDOR_AAS})

