from sqlalchemy import text, Connection
from typing import Dict, Any, List, Callable
from datetime import datetime, date, time
from numbers import Number
from .exceptions import ProfuturoException
from ._helpers import sub_anverso_tables, group_by
import calendar
import sys
import pandas as pd
import polars as pl


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


def extract_indicator(
    origin: Connection,
    destination: Connection,
    query: str,
    index: int,
    term: int,
    params: Dict[str, Any] = None,
    limit: int = None,
    partition_size: int = 1_000,
):
    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    try:
        cursor = origin.execute(text(query), params)
        for i, batch in enumerate(cursor.partitions(partition_size)):
            print(f"Updating records {i * partition_size} throught {(i + 1) * partition_size}")

            for value, accounts in group_by(batch, lambda row: row[1], lambda row: row[0]).items():
                destination.execute(text("""
                UPDATE "TCHECHOS_CLIENTE"
                SET "FTO_INDICADORES" = jsonb_set("FTO_INDICADORES", :field, :value)
                WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
                """), {
                    "field": f"{{{index}}}",
                    "value": f'"{value}"',
                    "accounts": tuple(accounts),
                    "term": term,
                })
    except Exception as e:
        raise ProfuturoException("TABLE_SWITCH_ERROR", term) from e


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
    origin: str,
    destination: str,
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
        df_pl = pl.read_database(_replace_query_params(query, params), origin)

        if term:
            df_pl = df_pl.with_columns(pl.lit(term).alias("FCN_ID_PERIODO"))

        if table in sub_anverso_tables():
            df_pl = df_pl.with_columns(pl.lit(datetime.now()).alias("FTD_FECHAHORA_ALTA"))

        df_pd = df_pl.to_pandas(use_pyarrow_extension_array=True)

        # We need to cast the PyArrow datetime to Pandas datetime
        for column, schema in df_pl.schema.items():
            if schema.is_(pl.Datetime) or schema.is_(pl.Date) or schema.is_(pl.Time):
                df_pd[column] = pd.to_datetime(df_pd[column])

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
    print(df_pl.schema)


def upsert_dataset(
    origin: Connection,
    destination: Connection,
    select_query: str,
    upsert_query: str,
    upsert_values: Callable[[int], List[str]],
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

            query = upsert_query.replace('(...)', _upsert_values_sentence(upsert_values, len(batch)))

            params = {}
            for j, row in enumerate(batch):
                for key, value in row._mapping.items():
                    params[f"{key}_{j}"] = value

            destination.execute(text(query), {**upsert_params, **params})
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done upserting {table}!")


def _replace_query_params(sql: str, params: Dict[str, Any]):
    statement = sql

    for key, value in params.items():
        if isinstance(value, (datetime, date, time)):
            formatted_value = f"date '{value.isoformat()}'"
        elif isinstance(value, Number):
            formatted_value = value
        else:
            formatted_value = f"'{value}'"

        statement = statement.replace(f':{key}', formatted_value)

    return statement


def _upsert_values_sentence(builder: Callable[[int], List[str]], record_count: int):
    values_sentences = []

    for i in range(record_count):
        values = builder(i)
        values_sentences.append("(" + ",".join(values) + ")")

    return ",".join(values_sentences)
