from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_integrity_pool
from profuturo.movements import extract_movements_integrity, extract_movements_mit
from profuturo.extraction import extract_terms
from profuturo.reporters import HtmlReporter
from sqlalchemy import text
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
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        records = postgres.execute(text("""
        SELECT mr."FTN_ID_SOLICITUD_REPROCESO", mr."FCN_ID_MUESTRA", mp."FTN_ID_MOVIMIENTO_PROFUTURO", mp."FTC_ORIGEN"
        FROM "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR" mr
            INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" mpc ON mr."FCN_ID_MOVIMIENTO_CONSAR" = mpc."FCN_ID_MOVIMIENTO_CONSAR"
            INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp ON mpc."FCN_ID_MOVIMIENTO_PROFUTURO" = mp."FTN_ID_MOVIMIENTO_PROFUTURO"
            INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" m ON m."FTN_ID_MUESTRA" = mr."FCN_ID_MUESTRA"
            -- INNER JOIN "GESTOR"."TCGESPRO_PERIODO_AREA" pa ON pa."FCN_ID_PERIODO" = m."FCN_ID_PERIODO"
        WHERE mr."FTC_STATUS" = 'Aprobado'
          -- AND mp."FTB_SWITCH" = true
          -- AND pa."FTB_ESTATUS" = true
        """)).all()

        reprocess = list({record[0] for record in records})
        samples = list({record[1] for record in records})
        movements_integrity = list({record[2] for record in filter(lambda record: record[3] == "INTEGRITY", records)})
        print(movements_integrity, bool(movements_integrity))
        movements_mit = list({record[2] for record in filter(lambda record: record[3] == "MIT", records)})
        print(movements_mit,bool(movements_integrity))

        if movements_mit:
            extract_movements_mit(postgres, term_id, start_month, end_month, movements_mit)
        else:
            notify(
                postgres,
                "Reproceso Movimientos MIT",
                phase,
                area,
                term=term_id,
                message=f"Se no se encontraron movimientos mit a reprocesar en los conceptos consar",
            )

        if movements_integrity:
            extract_movements_integrity(postgres, term_id, start_month, end_month, movements_integrity)
        else:
            notify(
                postgres,
                "Reproceso Movimientos MIT",
                phase,
                area,
                term=term_id,
                message=f"Se no se encontraron movimientos integrity a reprocesar en los conceptos consar",
            )

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
        report_mit_1 = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   MC."FTC_DESCRIPCION" AS CONSAR,
                   COUNT(DISTINCT M."FCN_CUENTA") AS CLIENTES,
                   SUM(M."FTF_MONTO_PESOS") AS IMPORTE
            FROM "TTHECHOS_MOVIMIENTO" M
                INNER JOIN "TCHECHOS_CLIENTE" I ON M."FCN_CUENTA" = I."FCN_CUENTA"
                INNER JOIN "TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON M."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
                INNER JOIN "TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FTN_ID_MOVIMIENTO_CONSAR"
            WHERE M."FCN_ID_PERIODO" = :term
              AND M."FCN_ID_CONCEPTO_MOVIMIENTO" = ANY(:movements)
            GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", MC."FTC_DESCRIPCION"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación", "CONSAR"],
            ["Registros", "Importe"],
            params={"term": term_id, "movements": movements_mit},
        )
        report_mit_2 = html_reporter.generate(
            postgres,
            """
            --movimientos postgres
            SELECT g."FTC_PERIODO" AS PERIODO,
                   s."FTC_DESCRIPCION" AS SIEFORE,
                   sb."FCC_VALOR" AS SUBCUENTA,
                   m."FCN_ID_TIPO_MOVIMIENTO",
                   ROUND(cast(SUM (m."FTF_MONTO_PESOS") as numeric(16,2)),2) as MONTO_PESOS
            FROM "HECHOS"."TTHECHOS_MOVIMIENTO" m
                INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" s ON m."FCN_ID_SIEFORE" = s."FTN_ID_SIEFORE"
                --INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp ON mp."FTN_ID_MOVIMIENTO_PROFUTURO" = m."FCN_ID_CONCEPTO_MOVIMIENTO"
                INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" sb ON m."FCN_ID_TIPO_SUBCTA" = sb."FTN_ID_TIPO_SUBCTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" g ON g."FTN_ID_PERIODO" = m."FCN_ID_PERIODO"
            WHERE m."FCN_ID_PERIODO" = :term
              AND m."FCN_ID_CONCEPTO_MOVIMIENTO" = ANY(:movements)
            GROUP BY g."FTC_PERIODO", s."FTC_DESCRIPCION", sb."FCC_VALOR", m."FCN_ID_TIPO_MOVIMIENTO"
            ORDER BY s."FTC_DESCRIPCION", sb."FCC_VALOR"
            """,
            ["PERIODO", "SIEFORE", "SUBCUENTA"],
            ["MONTO_PESOS"],
            params={"term": term_id, "movements": movements_mit},
        )
        report_integrity = html_reporter.generate(
            postgres,
            """
            --movimientos postgres
            SELECT g."FTC_PERIODO" AS PERIODO,
                   m."CVE_SIEFORE" AS SIEFORE_INTEGRITY,
                   m."SUBCUENTA" AS SUBCUENTA,
                   round(cast(sum(m."MONTO") as numeric(16, 2)), 2) AS MONTO_PESOS
            FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY" m
                -- LEFT JOIN "MAESTROS"."TCDATMAE_SIEFORE" s ON m."FCN_ID_SIEFORE" = s."FTN_ID_SIEFORE"
                -- INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp ON mp."FTN_ID_MOVIMIENTO_PROFUTURO" = m."FCN_ID_CONCEPTO_MOVIMIENTO"
                LEFT JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" sb ON m."SUBCUENTA" = sb."FTN_ID_TIPO_SUBCTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" g ON g."FTN_ID_PERIODO" = m."FCN_ID_PERIODO"
            WHERE m."FCN_ID_PERIODO" = :term
              AND m."CSIE1_CODMOV" = ANY((:movements)::varchar[])
            GROUP BY g."FTC_PERIODO", m."CVE_SIEFORE", m."SUBCUENTA"
            """,
            ["PERIODO", "SIEFORE", "SUBCUENTA"],
            ["MONTO_PESOS"],
            params={"term": term_id, "movements": movements_integrity},
        )

        notify(
            postgres,
            "Reproceso Movimientos MIT",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para movimientos exitosamente para el periodo",
            details=report_mit_1,
        )
        notify(
            postgres,
            "Reproceso Movimientos MIT",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para movimientos exitosamente para el periodo",
            details=report_mit_2,
        )
        notify(
            postgres,
            "Reproceso Movimientos Integrity",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de control para movimientos integrity exitosamente",
            details=report_integrity,
        )
