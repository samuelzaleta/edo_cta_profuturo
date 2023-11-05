from profuturo.common import truncate_table
from profuturo.database import configure_mit_spark, configure_postgres_spark, configure_integrity_spark
from profuturo.extraction import extract_dataset_spark
from profuturo.rdb import RdbJayDeBeApiDialect
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.engine import Connection
from sqlalchemy import text, select, table, column, literal
from pyspark.sql.functions import when, lit
from pyspark.sql import DataFrame
from typing import Callable, List
from datetime import datetime


def extract_movements_mit(
    postgres: Connection,
    term: int,
    start: datetime,
    end: datetime,
    movements: List[int] = None,
):
    if movements is None:
        truncate_table(postgres, 'TTHECHOS_MOVIMIENTO', term=term)
    else:
        postgres.execute(text("""
        DELETE FROM "HECHOS"."TTHECHOS_MOVIMIENTO"
        WHERE "FCN_ID_PERIODO" = :term
          AND "FCN_ID_CONCEPTO_MOVIMIENTO" = ANY(:movements)
        """), {"term": term, "movements": movements})

    for table_name in ["TTAFOGRAL_MOV_AVOL", "TTAFOGRAL_MOV_RCV", "TTAFOGRAL_MOV_COMP"]:
        _extract_table_movements_mit(table_name, term, start, end, True, movements)

    for table_name in ["TTAFOGRAL_MOV_BONO", "TTAFOGRAL_MOV_GOB", "TTAFOGRAL_MOV_SAR", "TTAFOGRAL_MOV_VIV"]:
        _extract_table_movements_mit(table_name, term, start, end, False, movements)


def extract_movements_integrity(
    postgres: Connection,
    term: int,
    start: datetime,
    end: datetime,
    movements: List[int] = None,
):
    if movements is None:
        truncate_table(postgres, 'TTHECHOS_MOVIMIENTOS_INTEGRITY', term=term)
    else:
        postgres.execute(text("""
        DELETE FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY"
        WHERE "FCN_ID_PERIODO" = :term
          AND "CSIE1_CODMOV" = ANY(:movements)
        """), {"term": term, "movements": movements})

    # SWITCH
    switches = postgres.execute(text("""
    SELECT DISTINCT "FTN_ID_MOVIMIENTO_PROFUTURO", "FTN_MONPES", "FCN_ID_TIPO_SUBCUENTA"
    FROM "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO"
    WHERE "FTB_SWITCH" = TRUE
      AND "FTC_ORIGEN" = 'INTEGRITY'
    """)).fetchall()

    normal_tables = {
        "MOV_GOBIERNO": [1, 3, 5, 8, 9],
        "MOV_VIV97": [1],
        "MOV_COMRET": [1, 3],
        "MOV_SAR92": [1, 2],
        "MOV_VIV92": [3],
        "MOV_BONO_UDI": [1, 3],
    }
    for table_name, monpes in normal_tables.items():
        _extract_table_movements_integrity(table_name, term, start, end, lambda df: _transform_mov(df, switches), monpes, movements)

    for table_name, monpes in {"MOV_RCV": [1, 2, 3, 4, 7, 8]}.items():
        _extract_table_movements_integrity(table_name, term, start, end, lambda df: _transform_rcv(df, switches), monpes, movements)


def _extract_table_movements_mit(table_name: str, term: int, start: datetime, end: datetime, with_reference: bool, movements=None):
    if movements is None:
        movements = []

    query = select(
            column("FTN_NUM_CTA_INVDUAL").label("FCN_CUENTA"),
            column("FCN_ID_TIPO_MOV").label("FCN_ID_TIPO_MOVIMIENTO"),
            column("FCN_ID_CONCEPTO_MOV").label("FCN_ID_CONCEPTO_MOVIMIENTO"),
            column("FCN_ID_TIPO_SUBCTA"),
            column("FCN_ID_SIEFORE"),
            column("FTC_FOLIO"),
            column("FTF_MONTO_ACCIONES"),
            text("ROUND(FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS"),
            column("FTD_FEH_LIQUIDACION"),
            literal("M").label("FTC_BD_ORIGEN"),
        ) \
        .select_from(table(table_name)) \
        .where(column("FTD_FEH_LIQUIDACION").between(start, end))

    if with_reference:
        query = query.add_columns(column("FNN_ID_REFERENCIA").label("FTN_REFERENCIA"))
    if len(movements) > 0:
        query = query.where(column("FCN_ID_CONCEPTO_MOV").in_(movements))

    extract_dataset_spark(
        configure_mit_spark,
        configure_postgres_spark,
        query.compile(dialect=OracleDialect(), compile_kwargs={"render_postcompile": True}),
        table='"HECHOS"."TTHECHOS_MOVIMIENTO"',
        term=term,
    )


def _extract_table_movements_integrity(
    table_name: str,
    term: int,
    start: datetime,
    end: datetime,
    transform: Callable[[DataFrame], DataFrame],
    monpes: List[int],
    movements: List[int] = None,
):
    if movements is None:
        movements = [
            106, 109, 129, 129, 210, 210, 260, 260, 405, 406, 410, 410, 412, 413, 414, 416, 420, 420, 421, 423, 424,
            426, 430, 430, 430, 433, 436, 440, 440, 441, 442, 443, 444, 446, 450, 450, 450, 450, 450, 450, 452, 453,
            453, 454, 454, 456, 470, 472, 474, 476, 610, 630, 710, 710, 760, 760, 805, 806, 841
        ]

    query = select(
            column("CSIE1_NUMCUE"), column("CSIE1_CODMOV"), column("CSIE1_FECCON"), column("CVE_SIEFORE"),
            column("CVE_SERVICIO"), column("CSIE1_NSSEMP"), column("CSIE1_FECHA_2"), column("CSIE1_FECTRA"),
            column("CSIE1_SECLOT"), column("CSIE1_CORREL"), column("CSIE1_PERPAG"), column("CSIE1_FOLSUA"),
            column("CSIE1_FECPAG"), column("CSIE1_FECPRO"),
        ) \
        .select_from(table(table_name)) \
        .where(column("CSIE1_FECCON") >= 20230101) \
        .where(column("CSIE1_FECCON") <= 20230131) \
        .where(column("CSIE1_CODMOV").in_(movements))

    for number in monpes:
        query.add_columns(column(f"CSIE1_MONPES_{number}"))

    extract_dataset_spark(
        configure_integrity_spark("cierren"),
        configure_postgres_spark,
        query.compile(dialect=RdbJayDeBeApiDialect(), compile_kwargs={"render_postcompile": True}),
        table='"HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY"',
        term=term,
        transform=transform,
    )


def _transform_rcv(df: DataFrame, switches) -> DataFrame:
    # Transformaciones adicionales de MOV_RCV
    df.withColumn("CSIE1_CODMOV", when(
        (df["CSIE1_CODMOV"] >= 114) & (df["CSIE1_NSSEMP"] == "INT. RET08"),
        117
    ).otherwise(df("CSIE1_CODMOV")))
    df.withColumn("CSIE1_CODMOV", when(
        (df["CSIE1_CODMOV"] >= 114) & (df["CSIE1_NSSEMP"] == "INT. CYVTRA"),
        117
    ).otherwise(df["CSIE1_CODMOV"]))

    return _transform_mov(df, switches)


def _transform_mov(df: DataFrame, switches) -> DataFrame:
    # ¿Afiliado o asignado?
    df.withColumn("SAL-AFIL-ASIG", when(df["CSIE1_NUMCUE"] < 7_000_000_000, 1).otherwise(2))

    # Si la SIEFORE es 98, toma el valor de 14
    df.withColumn("CVE_SIEFORE", when(df["CVE_SIEFORE"] == 98, 14).otherwise(df["CVE_SIEFORE"]))
    # Si la SIEFORE es 99, toma el valor de 15
    df.withColumn("CVE_SIEFORE", when(df["CVE_SIEFORE"] == 99, 15).otherwise(df["CVE_SIEFORE"]))

    df.withColumn("SUBCUENTA", lit(0))
    df.withColumn("MONTO", lit(0))
    # Extrae los MONPES en base a los switches
    for switch in switches:
        codmov = switch[0]
        monpes = "CSIE1_MONPES_" + str(switch[1])
        subcuenta = switch[2]

        if monpes not in df.columns:
            continue

        df.withColumn("SUBCUENTA", when(
            (df["CSIE1_CODMOV"] == codmov) & (df[monpes] > 0),
            subcuenta,
        ).otherwise(df["SUBCUENTA"]))
        df.withColumn("MONTO", when(
            (df["CSIE1_CODMOV"] == codmov) & (df[monpes] > 0),
            df[monpes],
        ).otherwise(df["MONTO"]))

    return df
