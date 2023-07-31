from pyspark.sql import SparkSession, DataFrame as SparkDataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql.functions import lit
from sqlalchemy import text, Connection, Row
from pandas import DataFrame as PandasDataFrame
from typing import Dict, Any, List, Callable, Sequence
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
    update_indicator(origin, destination, query, """
        UPDATE "TCHECHOS_CLIENTE"
        SET "FTO_INDICADORES" = jsonb_set("FTO_INDICADORES", :field, :value)
        WHERE "FCN_CUENTA" IN :accounts AND "FCN_ID_PERIODO" = :term
    """, term, select_params=params, update_params={"field": f"{{{index}}}"}, limit=limit, partition_size=partition_size)


def update_indicator(
    origin: Connection,
    destination: Connection,
    select_query: str,
    update_query: str,
    term: int,
    select_params: Dict[str, Any] = None,
    update_params: Dict[str, Any] = None,
    limit: int = None,
    partition_size: int = 1_000,
):
    if select_params is None:
        select_params = {}
    if update_params is None:
        update_params = {}
    if limit is not None:
        select_query = f"SELECT * FROM ({select_query}) WHERE ROWNUM <= :limit"
        select_params["limit"] = limit

    try:
        cursor = origin.execute(text(select_query), select_params)
        for i, batch in enumerate(cursor.partitions(partition_size)):
            print(f"Updating records {i * partition_size} through {(i + 1) * partition_size}")

            for value, accounts in group_by(batch, lambda row: row[1], lambda row: row[0]).items():
                destination.execute(text(update_query), {
                    **update_params,
                    "value": value,
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
    transform: Callable[[PandasDataFrame], PandasDataFrame] = None,
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
            df_pd = df_pd.assign(FTD_FECHAHORA_ALTA=datetime.now())

        if transform is not None:
            df_pd = transform(df_pd)

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


def extract_dataset_spark(
    origin_configurator: Callable[[DataFrameReader], DataFrameReader],
    destination_configurator: Callable[[DataFrameWriter], DataFrameWriter],
    query: str,
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
    transform: Callable[[SparkDataFrame], SparkDataFrame] = None,
):
    spark = SparkSession.builder \
        .master('local') \
        .appName("profuturo") \
        .getOrCreate()

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
        df_sp = origin_configurator(spark.read) \
            .format("jdbc") \
            .option("dbtable", f"({_replace_query_params(query, params)}) dataset") \
            .load()

        if term:
            df_sp = df_sp.withColumn("FCN_ID_PERIODO", lit(term))
        if table in sub_anverso_tables():
            df_sp = df_sp.withColumn("FTD_FECHAHORA_ALTA", lit(datetime.now()))

        if transform is not None:
            df_sp = transform(df_sp)

        destination_configurator(df_sp.write) \
            .format("jdbc") \
            .mode("append") \
            .option("dbtable", f'"{table}"') \
            .save()
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done extracting {table}!")
    print(df_sp.show())


def extract_dataset_polars(
    origin: str,
    destination: Connection,
    query: str,
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
    transform: Callable[[PandasDataFrame], PandasDataFrame] = None,
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

        if transform is not None:
            df_pd = transform(df_pd)

        df_pd.to_sql(
            table,
            destination,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=100,
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

            batch_set = list(_deduplicate_records(batch))
            query = upsert_query.replace('(...)', _upsert_values_sentence(upsert_values, len(batch_set)))

            params = {}
            for j, row in enumerate(batch_set):
                for key, value in row._mapping.items():
                    params[f"{key}_{j}"] = value

            destination.execute(text(query), {**upsert_params, **params})
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done upserting {table}!")


def _deduplicate_records(records: Sequence[Row]):
    ids = set()

    for record in records:
        if record[0] in ids:
            continue

        ids.add(record[0])
        yield record


def _replace_query_params(sql: str, params: Dict[str, Any]):
    statement = sql

    for key, value in params.items():
        if value is None:
            formatted_value = 'NULL'
        elif isinstance(value, (datetime, date, time)):
            formatted_value = f"date '{value.isoformat()}'"
        elif isinstance(value, Number):
            formatted_value = str(value)
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
