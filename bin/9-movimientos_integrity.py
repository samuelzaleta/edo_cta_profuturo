from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_integrity_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import text, CursorResult
import numpy as  np
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

    df["SUBCUENTA"] = ''
    df["MONTO"] = 0
    # Extrae los MONPES en base a los switches
    for switch in switches:
        codmov = switch[0]
        monpes = "CSIE1_" + switch[1]
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


load_dotenv()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
integrity_pool = get_integrity_pool("cierren")

phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, integrity_pool) as (postgres, integrity):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    cursor: CursorResult = postgres.execute(text("""
    SELECT "FTN_CODMOV", "FTC_TIPO_MONTO", "FTC_SUBCUENTA"
    FROM "TEST_SWITCH"
    WHERE "FTB_ACTIVO" = TRUE
    """))
    switches = cursor.fetchall()

    with register_time(postgres_pool, phase, term=term_id):
        truncate_table(postgres, "TEST_MOVIMIENTOS", term=term_id)
        extract_dataset(integrity, postgres, """
        SELECT CSIE1_NUMCUE AS FCN_CUENTA, 
               CSIE1_CODMOV,
               CSIE1_FECCON,
               CVE_SIEFORE AS FCN_ID_SIEFORE,
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
        """, "TEST_MOVIMIENTOS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
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
        """, "TEST_MOVIMIENTOS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform_rcv)
        extract_dataset(integrity, postgres, """
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
        """, "TEST_MOVIMIENTOS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres, """
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
        """, "TEST_MOVIMIENTOS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres, """
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
        """, "TEST_MOVIMIENTOS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres, """
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
        """, "TEST_MOVIMIENTOS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
        extract_dataset(integrity, postgres, """
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
        """, "TEST_MOVIMIENTOS", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)
