from profuturo.common import register_time, define_extraction, truncate_table, notify
from profuturo.database import get_postgres_pool, get_integrity_pool, get_postgres_oci_pool, configure_postgres_spark, configure_postgres_oci_spark
from profuturo.extraction import extract_terms, extract_dataset, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from pandas import DataFrame
from sqlalchemy import text
import numpy as np
import sys
import time


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


phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    start = start_month.strftime("%Y%m%d")
    end = end_month.strftime("%Y%m%d")
    # SWITCH
    switches = postgres.execute(text("""
    SELECT DISTINCT "FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES", "FCN_ID_TIPO_SUBCUENTA"
    FROM "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO"
    WHERE "FTB_SWITCH" = TRUE
        AND "FTC_ORIGEN" = 'INTEGRITY'
    """)).fetchall()

    movimientos_query_2 = (
    129, 116, 146, 412, 432, 434, 436, 442, 472, 474, 514, 520, 616, 646, 724, 727, 905, 915, 917, 923, 925, 926,
    927, 932,936, 942, 943, 945, 946, 947, 952, 955, 956, 957, 960, 962, 963, 964, 965, 966, 972, 973, 974, 109,
    136, 216, 260, 405,406, 409, 410, 413, 414, 416, 420, 421, 423, 424, 426, 430, 433, 440, 441, 443, 444, 446,
    450, 452, 453, 454, 456, 470, 476, 636, 710, 716, 760, 809, 914, 922, 924, 930, 933, 934, 944, 954, 921)

    movimientos_query_1 = (106, 110, 120, 127, 130, 140, 141, 210, 220, 720, 805, 806, 822, 824, 841)

    tablas_campos = {
        "MOV_AVOL": """CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO,CSIE1_MONPES_1,
                        CSIE1_MONPES_3, CSIE1_MONPES_5, CSIE1_NSSEMP, CSIE1_FECHA_2, CSIE1_FECTRA,
                        CSIE1_SECLOT, CSIE1_CORREL, CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
                        """,
        "MOV_BONO_UDI": """CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO, CSIE1_MONPES_1,
                            CSIE1_MONPES_3, CSIE1_NSSEMP, CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT,CSIE1_CORREL,
                            CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
                            """,
        "MOV_GOBIERNO": """CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO, CSIE1_MONPES_1,
                            CSIE1_MONPES_3, CSIE1_MONPES_5, CSIE1_MONPES_8, CSIE1_MONPES_9, CSIE1_NSSEMP,
                            CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL, CSIE1_PERPAG,
                            CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
                            """,
        "MOV_RCV": """CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO, CSIE1_MONPES_1,
                    CSIE1_MONPES_2, CSIE1_MONPES_3, CSIE1_MONPES_4, CSIE1_MONPES_7, CSIE1_MONPES_8,
                    CSIE1_NSSEMP, CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL, CSIE1_PERPAG,
                    CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO""",
        "MOV_VIV97": """CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO,CSIE1_MONPES_1,
                     CSIE1_NSSEMP, CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT, CSIE1_CORREL,
                     CSIE1_PERPAG,CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO""",

        "MOV_COMRET": """ CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO, CSIE1_MONPES_1,
                     CSIE1_MONPES_3, CSIE1_NSSEMP, CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_CORREL,CSIE1_PERPAG,
                     CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
                     """,
        "MOV_SAR92": """
                     CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO,
                     CSIE1_MONPES_1, CSIE1_MONPES_2, CSIE1_NSSEMP, CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT,
                     CSIE1_CORREL, CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
                     """,
        "MOV_VIV92": """CSIE1_NUMCUE, CSIE1_CODMOV, CSIE1_FECCON, CVE_SIEFORE, CVE_SERVICIO,
                     CSIE1_MONPES_3, CSIE1_NSSEMP, CSIE1_FECHA_2, CSIE1_FECTRA, CSIE1_SECLOT, 
                     CSIE1_CORREL, CSIE1_PERPAG, CSIE1_FOLSUA, CSIE1_FECPAG, CSIE1_FECPRO
                 """
    }

with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    start_month = term["start_month"]
    end_month = term["end_month"]
    start = start_month.strftime("%Y%m%d")
    end = end_month.strftime("%Y%m%d")

    with register_time(postgres_pool, phase, term_id, user, area):

        truncate_table(postgres_oci, 'TTHECHOS_MOVIMIENTOS_INTEGRITY', term=term_id)

        for tabla, columnas in tablas_campos.items():
            print(tabla)
            print(columnas)
            block = 0
            result = False
            print("Primer Query")

            while result == False:
                # Close and reopen the connection
                integrity_pool = get_integrity_pool("cierren")
                query_extract = f"""
                SELECT {columnas} FROM {tabla}
                WHERE CSIE1_FECCON >= {start}
                    AND CSIE1_FECCON <= {end}
                    AND (
                        (CSIE1_CODMOV = 213 AND CSIE1_NSSEMP = 'CAM-REG-BON') 
                        OR 
                        ( CSIE1_CODMOV IN (106, 110, 120, 127, 130, 140, 141, 210, 220, 720, 805, 806, 822, 824, 841)
                        AND (
                            CSIE1_NSSEMP = 'CAM-REG-BON'
                            OR CSIE1_NSSEMP = 'REINVERPNC'
                            OR CSIE1_NSSEMP = 'REINVER TRD'
                            OR CSIE1_NSSEMP = 'RECUPERIMSS'
                            OR LENGTH(TRIM(CSIE1_NSSEMP)) IN (10, 11)
                            OR CSIE1_NSSEMP = 'UNIFICADO'
                            )
                        )
                    )
                OFFSET {block * 10_000} ROWS FETCH NEXT 10000 ROWS ONLY
                """
                print("BLOCK",block)
                print("Query")
                print(query_extract)

                result = extract_dataset(
                    integrity_pool, postgres_oci, query_extract, 'TTHECHOS_MOVIMIENTOS_INTEGRITY',
                    term=term_id, params={
                        "start": start,
                        "end": end,
                    }, transform=transform
                )
                print("result:",result)

                # Close the connection
                integrity_pool.dispose()

                # Wait 3 seconds
                time.sleep(3)

                block += 1

            print("Segundo Query")
            block = 0
            while result == False:
                integrity_pool = get_integrity_pool("cierren")
                query_extract = f"""
                SELECT {columnas}
                FROM {tabla}
                WHERE CSIE1_CODMOV IN (    129, 116, 146, 412, 432, 434, 436, 442, 472, 474, 514, 520, 616, 646, 724, 727, 905, 915, 917, 923, 925, 926,
                927, 932,936, 942, 943, 945, 946, 947, 952, 955, 956, 957, 960, 962, 963, 964, 965, 966, 972, 973, 974, 109,
                136, 216, 260, 405,406, 409, 410, 413, 414, 416, 420, 421, 423, 424, 426, 430, 433, 440, 441, 443, 444, 446,
                450, 452, 453, 454, 456, 470, 476, 636, 710, 716, 760, 809, 914, 922, 924, 930, 933, 934, 944, 954, 921))
                    AND CSIE1_FECCON >= {start}
                    AND CSIE1_FECCON <= {end}
                OFFSET {block * 10_000} ROWS FETCH NEXT 10000 ROWS ONLY
                """
                print("BLOCK",block)
                print("Query")
                print(query_extract)
                result = extract_dataset(
                    integrity_pool, postgres_oci, query_extract, 'TTHECHOS_MOVIMIENTOS_INTEGRITY',
                    term=term_id, params={
                        "start": start,
                        "end": end
                    }, transform=transform
                )
                print("result:", result)
                # Close the connection
                integrity_pool.dispose()

                # Wait 3 seconds
                time.sleep(3)

                block += 1