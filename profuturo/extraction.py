from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import lit
from sqlalchemy import text, Connection, Row, RowMapping
from pandas import DataFrame as PandasDataFrame
from typing import Dict, Any, List, Callable, Sequence
from datetime import datetime, date, time
from numbers import Number
from .database import SparkConnectionConfigurator
from .exceptions import ProfuturoException
import calendar
import sys
import pandas as pd
import polars as pl


def extract_terms(conn: Connection, phase: int) -> Dict[str, Any]:
    try:
        term_id = int(sys.argv[2])
        cursor = conn.execute(text("""
        SELECT "FTC_PERIODO"
        FROM "TCGESPRO_PERIODO"
        WHERE "FTN_ID_PERIODO" = :term
        """), {"term": term_id})

        if cursor.rowcount == 0:
            raise ValueError("The term does not exist", phase)

        for row in cursor.fetchall():
            term = row[0].split('/')
            time_period = row[0]
            month = int(term[0])
            year = int(term[1])

            month_range = calendar.monthrange(year, month)
            start_month = date(year, month, 1)
            end_month = date(year, month, month_range[1])
            end_saldos = date(year, month + 1,1)
            valor_accion = date(year, month, month_range[1])

            print(f"Extracting period: from {start_month} to {end_month}")
            return {"id": term_id, "start_month": start_month, "end_month": end_month, "valor_accion": valor_accion, "end_saldos": end_saldos, "time_period": time_period}

        raise RuntimeError("Can not retrieve the term")
    except Exception as e:
        raise ProfuturoException("TERMS_ERROR", phase) from e


def update_indicator_spark(
    origin_configurator: SparkConnectionConfigurator,
    destination_configurator: SparkConnectionConfigurator,
    query: str,
    indicator: RowMapping,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
):
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        return df \
            .withColumn('FCN_ID_INDICADOR', lit(indicator["FTN_ID_INDICADOR"])) \
            .withColumn('FTN_EVALUA_INDICADOR', lit(
                (indicator["FTB_DISPONIBLE"] << 3) +
                (indicator["FTB_IMPRESION"] << 2) +
                (indicator["FTB_ENVIO"] << 1) +
                indicator["FTB_GENERACION"]
            ))

    try:
        extract_dataset_spark(
            origin_configurator,
            destination_configurator,
            query,
            '"HECHOS"."TCHECHOS_CLIENTE_INDICADOR"',
            term=term,
            params=params,
            limit=limit,
            transform=transform,
        )
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
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Extracting {table}...")

    try:
        df_pd = pd.read_sql_query(text(query), origin, params=params)
        df_pd = df_pd.rename(columns=str.upper)

        if term:
            df_pd = df_pd.assign(FCN_ID_PERIODO=term)

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


def extract_dataset_write_view_spark(
    origin: Connection,
    query: str,
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
    transform: Callable[[PandasDataFrame], PandasDataFrame] = None,
):
    spark = _get_spark_session()

    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Extracting {table}...")

    try:
        df_pd = pd.read_sql_query(text(query), origin, params=params)
        df_pd = df_pd.rename(columns=str.upper)

        if term:
            df_pd = df_pd.assign(FCN_ID_PERIODO=term)

        if transform is not None:
            df_pd = transform(df_pd)

        df_spark = spark.createDataFrame(df_pd)
        df_spark.createOrReplaceTempView(table)

        print("DONE VIEW")

    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e


def extract_dataset_spark(
    origin_configurator: SparkConnectionConfigurator,
    destination_configurator: SparkConnectionConfigurator,
    query: str,
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
    transform: Callable[[SparkDataFrame], SparkDataFrame] = None,
):
    spark = _get_spark_session()

    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Extracting {table}...")

    try:
        print("Creating dataframe...")
        df_sp = _create_spark_dataframe(spark, origin_configurator, query, params)
        print("Done dataframe!")

        if term:
            print("Adding period...")
            df_sp = df_sp.withColumn("FCN_ID_PERIODO", lit(term))
            print("Done adding period!")

        if transform is not None:
            print("Transforming dataframe...")
            df_sp = transform(df_sp)
            print("Done transforming dataframe!")

        print("Count:", df_sp.count())
        df_sp.show(10)
        print("Writing dataframe...")
        print("Schema", df_sp.schema)
        _write_spark_dataframe(df_sp, destination_configurator, table)
        print("Done writing dataframe!")
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done extracting {table}!")


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
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Extracting {table}...")

    try:
        # Utilizar Polars para leer los datos de SQL
        df_pl = pl.read_database(_replace_query_params(query, params), origin)

        if term:
            df_pl = df_pl.with_columns(pl.lit(term).alias("FCN_ID_PERIODO"))

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


def read_table_insert_temp_view(
    origin_configurator: SparkConnectionConfigurator,
    query: str,
    view: str,
    params: Dict[str, Any] = None
):
    if params is None:
        params = {}

    spark = _get_spark_session()
    print("EXTRACCIÓN")
    df = _create_spark_dataframe(spark, origin_configurator, query, params)
    print("DONE")
    df.createOrReplaceTempView(view)
    print("DONE VIEW:",view)
    df.show(2)
    print(df.count())


def _get_spark_session() -> SparkSession:
    return SparkSession.builder \
        .master('local[*]') \
        .appName("profuturo") \
        .config("spark.executor.memory", "32g") \
        .config("spark.driver.memory", "32g") \
        .config("spark.executor.instances", "5") \
        .config("spark.default.parallelism", "900") \
        .getOrCreate()


def _create_spark_dataframe(spark: SparkSession, connection_configurator, query: str, params: Dict[str, Any]) -> SparkDataFrame:
    return connection_configurator(spark.read) \
        .format("jdbc") \
        .option("dbtable", f"({_replace_query_params(query, params)}) dataset") \
        .option("numPartitions", 80) \
        .option("fetchsize", 100000) \
        .load()


def _write_spark_dataframe(df: SparkDataFrame, connection_configurator, table: str) -> None:
    connection_configurator(df.write) \
        .format("jdbc") \
        .mode("append") \
        .option("numPartitions", "20") \
        .option("fetchsize", "100000") \
        .option("batchsize", "100000") \
        .option("dbtable", f'{table}') \
        .save()
    spark = _get_spark_session()



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
