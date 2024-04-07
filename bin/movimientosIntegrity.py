from profuturo.common import notify, register_time, define_extraction, truncate_table
from profuturo.database import get_postgres_pool, get_integrity_pool, get_postgres_oci_pool
from profuturo.extraction import extract_terms, extract_dataset
from profuturo.reporters import HtmlReporter
from dotenv import load_dotenv
from datetime import datetime
from pandas import DataFrame
from sqlalchemy import text
import numpy as np
import sys

users = (10000790, 10000791, 10000792, 10000793, 10000794, 10000795, 10000797, 10000798, 10000799,
         10000800, 10000801, 10000802, 10000803, 10000804, 10000805, 10000806, 10000807, 10000808,
         10000809, 10000810, 10000811, 10000812, 10000813, 10000814, 10000815, 10000816, 10000817,
         10000818, 10000819, 10000820, 10000821, 10000822, 10000823, 10000824, 10000825, 10000826,
         10000827, 10000828, 10000830, 10000832, 10000833, 10000834, 10000835, 10000836, 10000837,
         10000838, 10000839, 10000840, 10000851, 10000861, 10000868, 10000872, 10000884, 10000885,
         10000887, 10000888, 10000889, 10000890, 10000891, 10000892, 10000893, 10000894, 10000895,
         10000896, 10000898, 10000899, 10000900, 10000901, 10000902, 10000903, 10000904, 10000905,
         10000906, 10000907, 10000908, 10000909, 10000910, 10000911, 10000912, 10000913, 10000914,
         10000915, 10000916, 10000917, 10000918, 10000919, 10000920, 10000921, 10000922, 10000923,
         10000924, 10000925, 10000927, 10000928, 10000929, 10000930, 10000931, 10000932, 10000933,
         10000934, 10000935, 10000936, 10000991, 10000992, 10000993, 10000994, 10000995, 10000996,
         10000997, 10000998, 10000999, 10001000, 10001001, 10001002, 10001003, 10001004, 10001005,
         10001006, 10001007, 10001008, 10001009, 10001010, 10001011, 10001012, 10001013, 10001014,
         10001015, 10001016, 10001017, 10001018, 10001019, 10001020, 10001021, 10001023, 10001024,
         10001025, 10001026, 10001027, 10001029, 10001030, 10001031, 10001032, 10001033, 10001034,
         10001035, 10001036, 10001037, 10001038, 10001039, 10001040, 10001041, 10001042, 10001098,
         10001099, 10001100, 10001101, 10001102, 10001103, 10001104, 10001105, 10001106, 10001107,
         10001108, 10001109, 10001110, 10001111, 10001112, 10001113, 10001114, 10001115, 10001116,
         10001117, 10001118, 10001119, 10001120, 10001121, 10001122, 10001123, 10001124, 10001125,
         10001126, 10001127, 10001128, 10001129, 10001130, 10001131, 10001132, 10001133, 10001134,
         10001135, 10001136, 10001137, 10001138, 10001139, 10001140, 10001141, 10001142, 10001143,
         10001145, 10001146, 10001147, 10001148, 10001263, 10001264, 10001265, 10001266, 10001267,
         10001268, 10001269, 10001270, 10001271, 10001272, 10001274, 10001275, 10001277, 10001278,
         10001279, 10001280, 10001281, 10001282, 10001283, 10001284, 10001285, 10001286, 10001288,
         10001289, 10001290, 10001292, 10001293, 10001294, 10001296, 10001297, 10001298, 10001299,
         10001300, 10001301, 10001305, 10001306, 10001307, 10001308, 10001309, 10001311, 10001312,
         10001314, 10001315, 10001316, 10001317, 10001318, 10001319, 10001320, 10001321, 10001322,
         1250002546, 1330029515, 1350011161, 1530002222, 1700004823, 3070006370, 3200089837, 3200231348,
         3200442678, 3200474366, 3200534369, 3200653979, 3200767640, 3200840759, 3200976872, 3200996343,
         3201096947, 3201292580, 3201368726, 3201423324, 3201693866, 3201895226, 3201900769, 3202077144,
         3202135111, 3202486462, 3300005489, 3300056170, 3300118473, 3300576485, 3300780661, 3300797020,
         3300797221, 3300809724, 3300835243, 3400764001, 3400958595, 3500053269, 3500058618, 6120000991,
         6442107959, 6442128265, 6449009395, 6449015130, 6130000050, 6449061689, 6449083099, 8051533577,
         8052970237, 8052710429)

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
          AND CSIE1_NUMCUE IN {users}
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
          AND CSIE1_NUMCUE IN {users}
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
          AND CSIE1_NUMCUE IN {users}
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
          AND CSIE1_NUMCUE IN {users}
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
         AND CSIE1_NUMCUE IN {users}
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
        AND CSIE1_NUMCUE IN {users}
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
        AND CSIE1_NUMCUE IN {users}
        """, "TTHECHOS_MOVIMIENTOS_INTEGRITY", term=term_id, params={
            "start": start_month.strftime("%Y%m%d"),
            "end": end_month.strftime("%Y%m%d"),
        }, transform=transform)

        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
            --movimientos postgres
            SELECT
            g."FTC_PERIODO" AS PERIODO,
            m."CVE_SIEFORE" AS SIEFORE_INTEGRITY,
            m."SUBCUENTA" AS SUBCUENTA,
            ROUND(cast(SUM (m."MONTO") as numeric(16,2)),2) as MONTO_PESOS
            FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY" m
            --LEFT JOIN "MAESTROS"."TCDATMAE_SIEFORE" s ON m."FCN_ID_SIEFORE" = s."FTN_ID_SIEFORE"
            --INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp ON mp."FTN_ID_MOVIMIENTO_PROFUTURO" = m."FCN_ID_CONCEPTO_MOVIMIENTO"
            LEFT JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" sb ON m."SUBCUENTA" = sb."FTN_ID_TIPO_SUBCTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" g ON g."FTN_ID_PERIODO" = m."FCN_ID_PERIODO"
            WHERE "FCN_ID_PERIODO" = :term
            GROUP BY
            g."FTC_PERIODO", m."CVE_SIEFORE", m."SUBCUENTA"
            """,
            ["PERIODO", "SIEFORE", "SUBCUENTA"],
            ["MONTO_PESOS"],
            params={"term": term_id},
        )
        #notify(
        #    postgres,
        #    f"Movimientos Integrity",
        #    phase,
        #    area,
        #    term=term_id,
        #    message="Se han generado las cifras de control para movimientos integrity exitosamente",
        #    details=report,
        #)
