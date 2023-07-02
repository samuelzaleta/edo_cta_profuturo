import polars as pl
from datetime import date
import calendar
import sys
from sqlalchemy import text, create_engine, engine
from typing import Dict, List, Any
from datetime import date
from sqlalchemy import text, Connection
from .exceptions import ProfuturoException
from ._helpers import group_by, chunk, sub_anverso_tables
import calendar
import sys
import datetime

def extract_terms(conn: engine.base.Connection, phase: int) -> Dict[str, Any]:
    try:
        term_id = sys.argv[2]
        cursor = conn.execute(text("""
        SELECT "FTC_PERIODO"
        FROM "TCGESPRO_PERIODO"
        WHERE "FTN_ID_PERIODO" = :term
        """), {"term": term_id})

        if cursor.rowcount == 0:
            raise ValueError("The term does not exist", phase)

        for row in cursor.fetchall():
            term = row[0].split('/')
            month = int(term[0])
            year = int(term[1])

            month_range = calendar.monthrange(year, month)
            start_month = date(year, month, 1)
            end_month = date(year, month, month_range[1])

            print(f"Extracting period: from {start_month} to {end_month}")
            return {"id": term_id, "start_month": start_month, "end_month": end_month}

        raise RuntimeError("Can not retrieve the term")
    except Exception as e:
        raise ProfuturoException("TERMS_ERROR", phase) from e


def extract_indicator(
    origin: engine.base.Connection,
    destination: engine.base.Connection,
    query: str,
    index: int,
    params: Dict[str, Any] = None,
    limit: int = None,
):
    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    try:
        cursor = origin.execute(text(query), params)
        for value, accounts in group_by(cursor.fetchall(), lambda row: row[1], lambda row: row[0]).items():
            for i, batch in enumerate(chunk(accounts, 1_000)):
                destination.execute(text("""
                UPDATE "TCDATMAE_CLIENTE"
                SET "FTO_INDICADORES" = jsonb_set("FTO_INDICADORES", :field, :value)
                WHERE "FTN_CUENTA" IN :accounts
                """), {
                    "accounts": tuple(batch),
                    "field": f"{{{index}}}",
                    "value": f'"{value}"',
                })

                print(f"Updating records {i * 1_000} through {(i + 1) * 1_000}")
    except Exception as e:
        raise ProfuturoException("TABLE_SWITCH_ERROR") from e


def extract_dataset(
    origin: Connection,
    destination: Connection,
    query: str,
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
):
    if params is None:
        params = {}
    if limit is not None:
        if table in sub_anverso_tables():
            query = f"SELECT Q.* FROM ({query}) AS Q LIMIT :limit"
            params["limit"] = limit
        else:
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
            params["limit"] = limit

    print(f"Extracting {table}...")

    try:
        # Utilizar Polars para leer los datos de SQL
        df_pl = pl.from_sql(origin, query, params=params)

        df_pl = df_pl.rename(str.upper)

        if term:
            df_pl = df_pl.with_column("FCN_ID_PERIODO", pl.lit(term))

        if table in sub_anverso_tables():
            df_pl = df_pl.with_column("FTD_FECHAHORA_ALTA", pl.lit(datetime.datetime.now()))

        df_pl.write_sql(
            table,
            destination,
            if_exists="append",
            index=False,
            method="multi",
            chunk_size=1_000,
        )
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done extracting {table}!")
    print(df_pl.schema)


def upsert_dataset(
    origin: engine.base.Connection,
    destination: engine.base.Connection,
    select_query: str,
    upsert_query: str,
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
):
    if params is None:
        params = {}
    if limit is not None:
        select_query = f"SELECT * FROM ({select_query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Upserting {table}...")

    try:
        cursor = origin.execute(text(select_query), params)

        for row in cursor.fetchall():
            destination.execute(text(upsert_query), row._mapping)
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done upserting {table}!")

