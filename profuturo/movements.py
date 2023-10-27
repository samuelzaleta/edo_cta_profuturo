from sqlalchemy.dialects.oracle.base import OracleDialect
from profuturo.database import configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_dataset_spark
from sqlalchemy import text, select, table, column, literal
from datetime import datetime


def extract_movements_mit(table_name: str, term: int, start_month: datetime, end_month: datetime, with_reference: bool, movements=None):
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
        .where(column("FTD_FEH_LIQUIDACION").between(start_month, end_month))

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
