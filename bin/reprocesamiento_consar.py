from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool
from profuturo.movements import extract_movements_mit
from profuturo.extraction import extract_terms
from profuturo.reporters import HtmlReporter
from sqlalchemy import text, Connection
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    postgres: Connection
    with register_time(postgres_pool, phase, term_id, user, area):
        records = postgres.execute(text("""
        SELECT mr."FTN_ID_SOLICITUD_REPROCESO", mr."FCN_ID_MUESTRA", mp."FTN_ID_MOVIMIENTO_PROFUTURO"
        FROM "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR" mr
            INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" mpc ON mr."FCN_ID_MOVIMIENTO_CONSAR" = mpc."FCN_ID_MOVIMIENTO_CONSAR"
            INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp ON mpc."FCN_ID_MOVIMIENTO_PROFUTURO" = mp."FTN_ID_MOVIMIENTO_PROFUTURO"
            INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" m ON m."FTN_ID_MUESTRA" = mr."FCN_ID_MUESTRA"
            -- INNER JOIN "GESTOR"."TCGESPRO_PERIODO_AREA" pa ON pa."FCN_ID_PERIODO" = m."FCN_ID_PERIODO"
        WHERE mr."FTC_STATUS" = 'Aprobado'
          -- AND mp."FTB_SWITCH" = true
          -- AND pa."FTB_ESTATUS" = true
        """)).all()

        reprocess = [record[0] for record in records]
        samples = [record[1] for record in records]
        movements = [record[2] for record in records]

        postgres.execute(text("""
        DELETE FROM "HECHOS"."TTHECHOS_MOVIMIENTO"
        WHERE "FCN_ID_PERIODO" = :term
          AND "FCN_ID_CONCEPTO_MOVIMIENTO" = ANY(:movements)
        """), {"term": term_id, "movements": movements})

        for table_name in ["TTAFOGRAL_MOV_AVOL", "TTAFOGRAL_MOV_RCV", "TTAFOGRAL_MOV_COMP"]:
            extract_movements_mit(table_name, term_id, start_month, end_month, True, movements)

        for table_name in ["TTAFOGRAL_MOV_BONO", "TTAFOGRAL_MOV_GOB", "TTAFOGRAL_MOV_SAR", "TTAFOGRAL_MOV_VIV"]:
            extract_movements_mit(table_name, term_id, start_month, end_month, False, movements)

        postgres.execute(text("""
        UPDATE "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR"
        SET "FTC_STATUS" = 'Reprocesado'
        WHERE "FTN_ID_SOLICITUD_REPROCESO" = ANY(:reprocess)
        """), {"reprocess": reprocess})

        # postgres.execute(text("""
        # DELETE FROM "GESTOR"."TCGESPRO_MUESTRA"
        # WHERE "FTN_ID_MUESTRA" IN :samples
        #   AND "FCN_ID_PERIODO" = :term
        # """), {"samples": samples, "term": term_id})

        # Cifras de control
        notify(
            postgres,
            f"Reprocesamiento Consar",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para reprocesamiento consar exitosamente para el periodo {time_period}",
        )
