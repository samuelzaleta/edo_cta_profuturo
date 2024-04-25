from profuturo.common import notify, register_time, define_extraction, truncate_table
from profuturo.database import get_postgres_pool, get_integrity_pool, get_postgres_oci_pool, configure_postgres_spark, configure_postgres_oci_spark
from profuturo.extraction import extract_terms, extract_dataset, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from dotenv import load_dotenv
from datetime import datetime
from pandas import DataFrame
from sqlalchemy import text
import numpy as np
import sys

def transform_rcv(df: DataFrame) -> DataFrame:
    # Transformaciones adicionales de MOV_RCV
    df["CSIE1_CODMOV"] = np.where(
        (df["CSIE1_CODMOV"] >= 114) & (df["CSIE1_NSSEMP"] == "INT. RET08"),
        117,
        df["CSIE1_CODMOV"],
    )
    df["CSIE1_CODMOV"] = np.where(
        (df["CSIE1_CODMOV"] >= 114) & (df["CSIE1_NSSEMP"] == "INT. CYVTRA"),
        117,
        df["CSIE1_CODMOV"],
    )

    return transform(df)


def transform(df: DataFrame) -> DataFrame:
    # ¿Afiliado o asignado?
    df["SAL-AFIL-ASIG"] = np.where(df["CSIE1_NUMCUE"] < 7_000_000_000, 1, 2)

    # Si la SIEFORE es 98, toma el valor de 14
    df["CVE_SIEFORE"] = np.where(df["CVE_SIEFORE"] == 98, 14, df["CVE_SIEFORE"])
    # Si la SIEFORE es 99, toma el valor de 15
    df["CVE_SIEFORE"] = np.where(df["CVE_SIEFORE"] == 99, 15, df["CVE_SIEFORE"])

    df["SUBCUENTA"] = 0
    df["MONTO"] = 0
    # Extrae los MONPES en base a los switches
    for switch in switches:
        codmov = switch[0]
        monpes = "CSIE1_MONPES_" + str(switch[1])
        subcuenta = switch[2]

        if monpes not in df.columns:
            continue

        df["SUBCUENTA"] = np.where(
            (df["CSIE1_CODMOV"] == codmov) & (df[monpes] > 0),
            subcuenta,
            df["SUBCUENTA"],
        )
        df["MONTO"] = np.where(
            (df["CSIE1_CODMOV"] == codmov) & (df[monpes] > 0),
            df[monpes],
            df["MONTO"]
        )

    return df


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
integrity_pool = get_integrity_pool("cierren")

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, integrity_pool,postgres_oci_pool) as (postgres, integrity, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        # SWITCH
        switches = postgres.execute(text("""
        SELECT DISTINCT "FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES", "FCN_ID_TIPO_SUBCUENTA"
        FROM "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO"
        WHERE "FTB_SWITCH" = TRUE
          AND "FTC_ORIGEN" = 'INTEGRITY'
        """)).fetchall()

        truncate_table(postgres_oci, 'TTHECHOS_MOVIMIENTOS_INTEGRITY', term=term_id)
        extract_dataset(integrity, postgres_oci, f"""
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
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres_oci, f"""
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
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform_rcv)
        extract_dataset(integrity, postgres_oci, f"""
        SELECT CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, 
               CVE_SIEFORE, CVE_SERVICIO, CSIE1_MONPES_1,
               CSIE1_NSSEMP, CSIE1_FECHA_2,
               CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL,
               CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
        FROM MOV_VIV97
        WHERE CSIE1_FECCON >= :start
          AND CSIE1_FECCON <= :end
          AND CSIE1_CODMOV IN (
              106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
              426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
              453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
          )
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres_oci, f"""
        SELECT CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE,
               CVE_SERVICIO, CSIE1_MONPES_1, CSIE1_MONPES_3, CSIE1_NSSEMP, 
               CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_CORREL,
               CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
        FROM MOV_COMRET
        WHERE CSIE1_FECCON >= :start
          AND CSIE1_FECCON <= :end
          AND CSIE1_CODMOV IN (
              106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
              426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
              453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
          )
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres_oci, f"""
        SELECT CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE,
               CVE_SERVICIO, CSIE1_MONPES_1, CSIE1_MONPES_2, CSIE1_NSSEMP, 
               CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL,
               CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
        FROM MOV_SAR92
        WHERE CSIE1_FECCON >= :start
          AND CSIE1_FECCON <= :end
          AND CSIE1_CODMOV IN (
              106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
              426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
              453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
          )
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres_oci, f"""
        SELECT CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE,
               CVE_SERVICIO, CSIE1_MONPES_3, CSIE1_NSSEMP, CSIE1_FECHA_2,
               CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL, CSIE1_PERPAG,
               CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
        FROM MOV_VIV92
        WHERE CSIE1_FECCON >= :start
          AND CSIE1_FECCON <= :end
          AND CSIE1_CODMOV IN (
              106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
              426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
              453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
        )
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres_oci, f"""
        SELECT CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE,
               CVE_SERVICIO, CSIE1_MONPES_1, CSIE1_MONPES_3, CSIE1_NSSEMP, 
               CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL, 
               CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
        FROM MOV_BONO_UDI
        WHERE CSIE1_FECCON >= :start
          AND CSIE1_FECCON <= :end
          AND CSIE1_CODMOV IN (
              106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424, 
              426, 430, 430, 430, 433, 436, 440, 440, 441,442, 443, 444, 446, 450, 450, 450, 450, 450, 450,452, 453, 
              453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
          )
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)

        postgres_oci.execute(
            text(""" DROP TABLE IF EXISTS "MAESTROS"."TCGESPRO_MOVIMIENTO_PROFUTURO" """))

        postgres_oci.execute(
            text(""" DROP TABLE IF EXISTS "MAESTROS"."TCGESPRO_PERIODO" """))

        postgres_oci.execute(
            text(""" DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" """))

        postgres_oci.execute(text("""
                                DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_SIEFORE"
                                """))

        # Extracción de tablas temporales
        query_temp = """
                       SELECT
                       "FTN_ID_SIEFORE", "FTC_DESCRIPCION", "FTC_DESCRIPCION_CORTA", "FTC_SIEFORE"
                       FROM "MAESTROS"."TCDATMAE_SIEFORE"
                       """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_SIEFORE"'
        )


        extract_dataset_spark(configure_postgres_spark, configure_postgres_oci_spark,
                              """ SELECT * FROM "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" """,
                              '"MAESTROS"."TCGESPRO_MOVIMIENTO_PROFUTURO"'
                              )

        extract_dataset_spark(configure_postgres_spark, configure_postgres_oci_spark,
                              """ SELECT * FROM "GESTOR"."TCGESPRO_PERIODO" """,
                              '"MAESTROS"."TCGESPRO_PERIODO"'
                              )

        extract_dataset_spark(configure_postgres_spark, configure_postgres_oci_spark,
                              """ SELECT * FROM "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" """,
                              '"MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"'
                              )


        # Cifras de control
        report = html_reporter.generate(
            postgres_oci,
            """
            --movimientos postgres
            SELECT
            g."FTC_PERIODO" AS PERIODO,
            s."FTC_DESCRIPCION" AS SIEFORE_INTEGRITY,
            ROUND(cast(SUM (m."MONTO") as numeric(16,2)),2) as MONTO_PESOS
            FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY" m
            LEFT JOIN "MAESTROS"."TCDATMAE_SIEFORE" s ON  s."FTN_ID_SIEFORE" =
            CASE m."CVE_SIEFORE"
            WHEN 7 THEN 74
            WHEN 98 THEN 80
            WHEN 99 THEN 81
            WHEN 2 THEN 82
            WHEN 4 THEN 83
            WHEN 8 THEN 6318
            WHEN 3 THEN 6319
            WHEN 1 THEN 6320
            WHEN 11 THEN 6322
            WHEN 5 THEN 6323
            WHEN 12 THEN 6324
            WHEN 6 THEN 6325
            WHEN 13 THEN 6326
            END
            INNER JOIN "MAESTROS"."TCGESPRO_PERIODO" g ON g."FTN_ID_PERIODO" = m."FCN_ID_PERIODO"
            WHERE "FCN_ID_PERIODO" = :term
            GROUP BY
            g."FTC_PERIODO", s."FTC_DESCRIPCION"
            """,
            ["PERIODO", "SIEFORE"],
            ["MONTO_PESOS"],
            params={"term": term_id},
        )
        notify(
            postgres,
            f"Movimientos Integrity",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de control para movimientos integrity exitosamente",
            details=report,
        )

        postgres_oci.execute(
            text(""" DROP TABLE IF EXISTS "MAESTROS"."TCGESPRO_MOVIMIENTO_PROFUTURO" """))

        postgres_oci.execute(
            text(""" DROP TABLE IF EXISTS "MAESTROS"."TCGESPRO_PERIODO" """))

        postgres_oci.execute(
            text(""" DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" """))

        postgres_oci.execute(text("""
                                       DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_SIEFORE"
                                       """))
