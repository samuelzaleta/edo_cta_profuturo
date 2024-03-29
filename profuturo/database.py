from typing import Union, Callable
from pyspark.sql import DataFrameReader, DataFrameWriter
from sqlalchemy.exc import OperationalError
from sqlalchemy import Engine
from contextlib import ExitStack, contextmanager
from .exceptions import ProfuturoException
import jaydebeapi
import sqlalchemy
import oracledb
import psycopg2
import os


SparkConnection = Union[DataFrameReader, DataFrameWriter]
SparkConnectionConfigurator = Callable[[SparkConnection, str, bool], SparkConnection]


@contextmanager
def use_pools(phase: int, *pools: Engine):
    with ExitStack() as stack:
        conns = []

        for pool in pools:
            try:
                conns.append(stack.enter_context(pool.connect()))
            except OperationalError as e:
                raise ProfuturoException("DATABASE_CONNECTION_ERROR", phase) from e

        yield conns


def get_mit_conn() -> oracledb.Connection:
    return oracledb.connect(
        host=os.getenv("MIT_HOST"),
        port=int(os.getenv("MIT_PORT")),
        service_name=os.getenv("MIT_DATABASE"),
        user=os.getenv("MIT_USER"),
        password=os.getenv("MIT_PASSWORD"),
    )


def get_buc_conn() -> oracledb.Connection:
    return oracledb.connect(
        host=os.getenv("BUC_HOST"),
        port=int(os.getenv("BUC_PORT")),
        service_name=os.getenv("BUC_DATABASE"),
        user=os.getenv("BUC_USER"),
        password=os.getenv("BUC_PASSWORD"),
    )


def get_postgres_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        options='-c search_path="MAESTROS","GESTOR","HECHOS","RESULTADOS"',
    )


def get_integrity_conn(database: str):
    host = '130.40.30.144'
    port = int(1730)
    user = 'SIEFORE'
    password = 'SIEFORE2019'

    print(host, port, user, password, database)

    return lambda: jaydebeapi.connect(
        "oracle.rdb.jdbc.rdbThin.Driver",
        f"jdbc:rdbThin://{host}:{port}/mexico$base:cierren",
        {"user": user, "password": password},
        "/opt/profuturo/libs/RDBTHIN.JAR"
    )


def get_mit_pool():
    oracledb.init_oracle_client()

    return sqlalchemy.create_engine(
        "oracle+oracledb://",
        creator=get_mit_conn,
    )


def get_buc_pool():
    oracledb.init_oracle_client()

    return sqlalchemy.create_engine(
        "oracle+oracledb://",
        creator=get_buc_conn,
    )


def get_integrity_pool(database: str):
    return sqlalchemy.create_engine(
        "rdb+jaydebeapi://",
        creator=get_integrity_conn(database),
    )


def get_postgres_pool():
    return sqlalchemy.create_engine(
        "postgresql+psycopg2://",
        creator=get_postgres_conn,
        execution_options={"isolation_level": "AUTOCOMMIT"},
    )


def configure_mit_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    host = os.getenv("MIT_HOST")
    port = int(os.getenv("MIT_PORT"))
    service_name = os.getenv("MIT_DATABASE")
    user = os.getenv("MIT_USER")
    password = os.getenv("MIT_PASSWORD")

    return configure_jdbc_spark(connection, table, reading) \
        .option("url", f"jdbc:oracle:thin:@//{host}:{port}/{service_name}") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("oracle.jdbc.timezoneAsRegion", False) \
        .option("user", user) \
        .option("password", password)


def configure_buc_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    host = '172.22.164.19'#os.getenv("BUC_HOST")
    port = 16161 #int(os.getenv("BUC_PORT"))
    service_name =  'QA34' #os.getenv("BUC_DATABASE")
    user = 'CLUNICO' #os.getenv("BUC_USER")
    password = 'temp4now13' #os.getenv("BUC_PASSWORD")

    return configure_jdbc_spark(connection, table, reading) \
        .option("url", f"jdbc:oracle:thin:@//{host}:{port}/{service_name}") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("oracle.jdbc.timezoneAsRegion", False) \
        .option("user", user) \
        .option("password", password)


def configure_integrity_spark(database: str) -> SparkConnectionConfigurator:
    host = '130.40.30.144'
    port = int(1714)
    user = 'SIEFORE'
    password = 'SIEFORE2019'

    return lambda connection, table, reading: configure_jdbc_spark(connection, table, reading) \
        .option("url", f"jdbc:rdbThin://{host}:{port}/mexico$base:{database}") \
        .option("driver", "oracle.rdb.jdbc.rdbThin.Driver") \
        .option("oracle.jdbc.timezoneAsRegion", False) \
        .option("user", user) \
        .option("password", password)


def configure_postgres_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT"))
    database = os.getenv("POSTGRES_DATABASE")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    return configure_jdbc_spark(connection, table, reading) \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("search_path", '"MAESTROS","GESTOR","HECHOS","RESULTADOS"') \
        .option("user", user) \
        .option("password", password)


def configure_bigquery_spark(connection: SparkConnection, table: str, _: bool) -> SparkConnection:
    return connection \
        .format("bigquery") \
        .option("temporaryGcsBucket", "dataproc-staging-us-central1-313676594114-h7pphtkf") \
        .option("table", table)


def configure_jdbc_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    if reading:
        connection = connection \
            .option("numPartitions", 80) \
            .option("fetchsize", 100000)
    else:
        connection = connection \
            .option("numPartitions", 20) \
            .option("fetchsize", 100000) \
            .option("batchsize", 100000)

    return connection \
        .format("jdbc") \
        .option("dbtable", table)


def get_mit_url():
    host = os.getenv("MIT_HOST")
    port = int(os.getenv("MIT_PORT"))
    service_name = os.getenv("MIT_DATABASE")
    user = os.getenv("MIT_USER")
    password = os.getenv("MIT_PASSWORD")

    return f"oracle://{user}:{password}@{host}:{port}/{service_name}"


def get_buc_url():
    host = os.getenv("BUC_HOST")
    port = int(os.getenv("BUC_PORT"))
    service_name = os.getenv("BUC_DATABASE")
    user = os.getenv("BUC_USER")
    password = os.getenv("BUC_PASSWORD")

    return f"oracle://{user}:{password}@{host}:{port}/{service_name}"


def get_postgres_url():
    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT"))
    database = os.getenv("POSTGRES_DATABASE")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    return f'postgresql://{user}:{password}@{host}:{port}/{database}'
