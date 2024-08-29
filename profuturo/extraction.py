from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import lit, array
from sqlalchemy.sql.compiler import Compiled
from sqlalchemy.engine import Row, RowMapping, Connection
from sqlalchemy import text
from pandas import DataFrame as PandasDataFrame
from typing import Dict, Any, List, Callable, Sequence, Union
from datetime import datetime, date, time
from .database import SparkConnectionConfigurator
from .exceptions import ProfuturoException
from dateutil.relativedelta import relativedelta
import calendar
from numbers import Number
import sys
import pandas as pd
import polars as pl


def extract_terms(conn: Connection, phase: int, term_id: int = None) -> Dict[str, Any]:
    try:
        term_id = term_id or int(sys.argv[2])

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
            valor_accion = date(year, month, month_range[1])
            #MENOS UN MES#
            end_saldos_anterior = start_month
            valor_accion_anterior = valor_accion - relativedelta(months=1)
            #MAS UN MES O DOS MESES#
            start_next_mes_valor_accion = start_month + relativedelta(months=1)
            if month == 4 or month == 12:
                start_next_mes = start_month + relativedelta(months=1) + relativedelta(days=1)
            else:
                start_next_mes = start_month + relativedelta(months=1)

            # Validar si start_next_mes es sábado o domingo
            weekday = start_next_mes.weekday()
            if weekday == calendar.SATURDAY:
                start_next_mes += relativedelta(days=2)
            elif weekday == calendar.SUNDAY:
                start_next_mes += relativedelta(days=1)

            print(f"Extracting period: from {start_month} to {end_month}")
            return {
                "id": term_id,
                "start_month": start_month,
                "end_month": end_month,
                "valor_accion": valor_accion,
                "time_period": time_period,
                "end_saldos_anterior": end_saldos_anterior,
                "valor_accion_anterior": valor_accion_anterior,
                "start_next_mes": start_next_mes,
                "start_next_mes_valor_accion": start_next_mes_valor_accion
            }

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
    area: int = None
):
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        print(indicator["FTB_DISPONIBLE"])
        return df \
            .withColumn('FCN_ID_INDICADOR', lit(indicator["FTN_ID_INDICADOR"])) \
            .withColumn('FCN_ID_AREA', lit(area)) \
                        .withColumn('FTA_EVALUA_INDICADOR', array(
            lit(indicator["FTB_DISPONIBLE"]),
            lit(indicator["FTB_ENVIO"]),
            lit(indicator["FTB_IMPRESION"]),
            lit(indicator["FTB_GENERACION"])
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

def create_dataset(
    origin: Connection,
    query: str,
    term: int = None,
    params: Dict[str, Any] = None,
    transform: Callable[[pd.DataFrame], pd.DataFrame] = None,
):
    if isinstance(query, Compiled):
        params = query.params
        query = str(query)
    if params is None:
        params = {}
    try:
        df_pd = pd.read_sql_query(text(query), origin, params=params)
        df_pd = df_pd.rename(columns=str.upper)

        if term:
            df_pd = df_pd.assign(FCN_ID_PERIODO=term)

        if transform is not None:
            df_pd = transform(df_pd)

        return df_pd
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e



def extract_dataset(
    origin: Connection,
    destination: Connection,
    query: str,
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    transform: Callable[[pd.DataFrame], pd.DataFrame] = None,
):
    if isinstance(query, Compiled):
        params = query.params
        query = str(query)
    if params is None:
        params = {}

    print(f"Extracting {table}...")

    try:
        df_pd = pd.read_sql_query(text(query), origin, params=params)
        df_pd = df_pd.rename(columns=str.upper)

        if term:
            df_pd = df_pd.assign(FCN_ID_PERIODO=term)

        if transform is not None:
            df_pd = transform(df_pd)

        #df_pd.to_sql(table, destination, if_exists="append", index=False, method="multi",chunksize=10_000)
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

    print(f"Done extracting {table}!")
    print(df_pd.head(20))
    print(df_pd.info())
    return df_pd.empty


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

def extract_dataset_spark_return_df(
        origin_configurator: SparkConnectionConfigurator,
        query: Union[str, Compiled],
        table: str,
        term: int = None,
        params: Dict[str, Any] = None,
        limit: int = None,
        transform: Callable[[SparkDataFrame], SparkDataFrame] = None,
) -> object:
    spark = _get_spark_session()

    if isinstance(query, Compiled):
        params = query.params
        query = str(query)
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
            df_sp = df_sp.withColumn("FCN_ID_PERIODO", lit(term))
            print("Done adding period!")

        if transform is not None:

            df_sp = transform(df_sp)
            print("Done transforming dataframe!")

        print("Count:", df_sp.count())
        return df_sp
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e


def extract_dataset_spark(
    origin_configurator: SparkConnectionConfigurator,
    destination_configurator: SparkConnectionConfigurator,
    query: Union[str, Compiled],
    table: str,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
    transform: Callable[[SparkDataFrame], SparkDataFrame] = None,
):
    spark = _get_spark_session()

    if isinstance(query, Compiled):
        params = query.params
        query = str(query)
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
    upsert_id: Callable[[Row], str] = None,
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

            batch_set = list(_deduplicate_records(batch, upsert_id))
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
    params: Dict[str, Any] = None,
    term: int = None,
    transform: Callable[[SparkDataFrame], SparkDataFrame] = None,
):
    if params is None:
        params = {}

    spark = _get_spark_session()
    print("EXTRACCIÓN")
    df = _create_spark_dataframe(spark, origin_configurator, query, params)
    print("DONE")

    if term:
        print("Adding period...")
        df = df.withColumn("FCN_ID_PERIODO", lit(term))
        print("Done adding period!")

    if transform is not None:
        print("Transforming dataframe...")
        df = transform(df)
        print("Done transforming dataframe!")

    df.createOrReplaceTempView(view)
    print("DONE VIEW:",view)
    df.show(2)


def _get_spark_session(
    excuetor_memory: str = '4g',
    memory_overhead: str ='1g',
    memory_offhead: str ='1g',
    driver_memory: str ='1g',
    intances: int = 2,
    cores: int =4,
    parallelims :int = 6000
) -> SparkSession:
    return SparkSession.builder \
        .appName("profuturo") \
        .config("spark.executor.memory", f"{excuetor_memory}") \
        .config("spark.executor.memoryOverhead", f"{memory_overhead}") \
        .config("spark.executor.memoryOffHeap", f"{memory_offhead}") \
        .config("spark.driver.memory", f"{driver_memory}") \
        .config("spark.executor.instances", f"{intances}") \
        .config("spark.default.parallelism", f"{parallelims}") \
        .config("spark.executor.cores", f"{cores}") \
        .getOrCreate()


def _create_spark_dataframe(
    spark: SparkSession,
    connection_configurator: SparkConnectionConfigurator,
    query: str,
    params: Dict[str, Any],
) -> SparkDataFrame:
    return connection_configurator(spark.read, _replace_query_params(query, params), True) \
        .load()


def _write_spark_dataframe(
    df: SparkDataFrame,
    connection_configurator: SparkConnectionConfigurator,
    table: str,
    mode="append"
) -> None:
    connection_configurator(df.write, table, False) \
        .mode(mode) \
        .option("numRowsPerSparkPartition", 40_000) \
        .option("compression", "snappy") \
        .save()


def _deduplicate_records(records: Sequence[Row], key_generator: Callable[[Row], str]):
    if not key_generator:
        key_generator = lambda row: row[0]

    upserted_ids = set()

    for record in records:
        upsert_id = key_generator(record)

        if upsert_id in upserted_ids:
            continue

        upserted_ids.add(upsert_id)
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
