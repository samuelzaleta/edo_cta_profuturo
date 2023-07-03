from sqlalchemy import text, Connection
from typing import Dict, List, Any
from datetime import date
from .exceptions import ProfuturoException
from ._helpers import group_by, chunk, sub_anverso_tables
import calendar
import sys
import pandas as pd
import datetime


def extract_terms(conn: Connection, phase: int) -> Dict[str, Any]:
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
            f"SELECT Q.* FROM ({query}) AS Q LIMIT :limit"
            params["limit"] = limit
        else:
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
            params["limit"] = limit

    print(f"Extracting {table}...")

    try:
        df_pd = pd.read_sql_query(text(query), origin, params=params)
        df_pd = df_pd.rename(columns=str.upper)

        if term:
            df_pd = df_pd.assign(FCN_ID_PERIODO=term)

        if table in sub_anverso_tables():
            df_pd = df_pd.assign(FTD_FECHAHORA_ALTA=datetime.datetime.now())

        df_pd.to_sql(
            table,
            destination,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1_000,
        )
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done extracting {table}!")
    print(df_pd.info())


def upsert_dataset(
    origin: Connection,
    destination: Connection,
    select_query: str,
    upsert_query: str,
    table: str,
    term: int = None,
    select_params: Dict[str, Any] = None,
    upsert_params: Dict[str, Any] = None,
    limit: int = None,
):
    if select_params is None:
        select_params = {}
    if upsert_params is None:
        upsert_params = {}
    if limit is not None:
        select_query = f"SELECT * FROM ({select_query})WHERE ROWNUM <= :limit"
        select_params["limit"] = limit

    print(f"Upserting {table}...")

    try:
        cursor = origin.execute(text(select_query), select_params)

        for row in cursor.fetchall():
            destination.execute(text(upsert_query), {**upsert_params, **row._mapping})
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done upserting {table}!")
