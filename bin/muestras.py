from sqlalchemy import text, Connection, CursorResult
from profuturo.common import register_time, define_extraction
from profuturo.database import get_postgres_pool
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys


def find_samples(samplesCursor: CursorResult):
    samples = set()
    i = 0
    for batch in samplesCursor.partitions(50):
        for record in batch:
            account = record[0]
            consars = record[1]
            i = i + 1

            for consar in consars:
                if consar not in configurations:
                    continue

                configurations[consar] = configurations[consar] - 1

                if configurations[consar] == 0:
                    del configurations[consar]

                samples.add(account)

                if len(configurations) == 0:
                    print("Cantidad de registros: " + str(i))
                    return samples

    raise Exception('Insufficient samples')


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
user = int(sys.argv[3])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    postgres: Connection

    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, area, usuario=user, term=term_id):
        # Extracción
        cursor = postgres.execute(text("""
        SELECT "FCN_ID_MOVIMIENTO_CONSAR", "FCN_NUM_MUESTRAS"
        FROM "GESTOR"."TEST_MUESTRAS_CONSAR"
        """))
        configurations = {configuration[0]: configuration[1] for configuration in cursor.fetchall()}

        cursor = postgres.execute(text("""
        SELECT "FCN_CUENTA", array_agg(CC."FCN_ID_MOVIMIENTO_CONSAR")
        FROM (
            SELECT M."FCN_CUENTA", PC."FCN_ID_MOVIMIENTO_CONSAR"
            FROM "HECHOS"."TTHECHOS_MOVIMIENTO" M
                INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON M."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
            WHERE M."FCN_ID_PERIODO" = :term
            GROUP BY M."FCN_CUENTA", PC."FCN_ID_MOVIMIENTO_CONSAR"
        ) AS CC
        INNER JOIN "GESTOR"."TEST_MUESTRAS_CONSAR" MC ON CC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FCN_ID_MOVIMIENTO_CONSAR"
        GROUP BY "FCN_CUENTA"
        ORDER BY sum(1.0 / "FCN_NUM_MUESTRAS") DESC
        """), {'term': term_id})
        samples = find_samples(cursor)

        print(samples)
        print(len(samples))
