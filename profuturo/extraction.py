from sqlalchemy import text, Connection
from typing import Dict, Any, List, Callable
from datetime import date
from .exceptions import ProfuturoException
from ._helpers import sub_anverso_tables
import calendar
import sys
import pandas as pd
import polars as pl
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


def extract_dataset_polars(
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
    origin: Connection,
    destination: Connection,
    select_query: str,
    upsert_query: str,
    table: str,
    term: int = None,
    select_params: Dict[str, Any] = None,
    upsert_params: Dict[str, Any] = None,
    limit: int = None,
    partition_size: int = 100,
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

        for i, batch in enumerate(cursor.partitions(partition_size)):
            print(f"Upserting records {i * partition_size} through {(i + 1) * partition_size}")

            params = {}
            for j, row in enumerate(batch):
                for key, value in row._mapping.items():
                    params[f"{key}_{j}"] = value

            destination.execute(text(upsert_query), {**upsert_params, **params})
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done upserting {table}!")


def upsert_values_sentence(builder: Callable[[int], List[str]], partition_size: int = 100):
    values_sentences = []

    for i in range(partition_size):
        values = builder(i)
        values_sentences.append("(" + ",".join(values) + ")")

    return ",".join(values_sentences)
