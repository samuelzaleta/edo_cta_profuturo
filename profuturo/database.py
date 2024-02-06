from typing import Union, Callable
from pyspark.sql import DataFrameReader, DataFrameWriter
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine
from contextlib import ExitStack, contextmanager
from .exceptions import ProfuturoException
import jaydebeapi
import sqlalchemy
import cx_Oracle
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


def get_mit_conn() -> cx_Oracle.Connection:
    dsn = cx_Oracle.makedsn(
        os.getenv("MIT_HOST"),
        int(os.getenv("MIT_PORT")),
        service_name=os.getenv("MIT_DATABASE"),
    )

    return cx_Oracle.connect(
        dsn=dsn,
        user=os.getenv("MIT_USER"),
        password=os.getenv("MIT_PASSWORD"),
    )


def get_buc_conn() -> cx_Oracle.Connection:
    dsn = cx_Oracle.makedsn(
        os.getenv("BUC_HOST"),
        int(os.getenv("BUC_PORT")),
        service_name=os.getenv("BUC_DATABASE"),
    )

    return cx_Oracle.connect(
        dsn=dsn,
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
        options='-c search_path="MAESTROS","GESTOR","HECHOS","RESULTADOS","ESTADO_CUENTA"',
    )


def get_integrity_conn(database: str):
    def creator():
        host = os.getenv("INTEGRITY_HOST")
        port = os.getenv("INTEGRITY_PORT")
        user = os.getenv("INTEGRITY_USER")
        password = os.getenv("INTEGRITY_PASSWORD")

        return jaydebeapi.connect(
            "oracle.rdb.jdbc.rdbThin.Driver",
            f"jdbc:rdbThin://{host}:{port}/mexico$base:{database}@transaction=readonly",
            {"user": user, "password": password},
            "/opt/profuturo/libs/RDBTHIN.JAR"
        )

    return creator


def get_mit_pool():
    cx_Oracle.init_oracle_client()

    return sqlalchemy.create_engine(
        "oracle+cx_oracle://",
        creator=get_mit_conn,
    )


def get_buc_pool():
    cx_Oracle.init_oracle_client()

    return sqlalchemy.create_engine(
        "oracle+cx_oracle://",
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


def get_bigquery_pool() -> Engine:
    return sqlalchemy.create_engine(
        "bigquery://"
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
    host = os.getenv("BUC_HOST")
    port = int(os.getenv("BUC_PORT"))
    service_name = os.getenv("BUC_DATABASE")
    user = os.getenv("BUC_USER")
    password = os.getenv("BUC_PASSWORD")

    return configure_jdbc_spark(connection, table, reading) \
        .option("url", f"jdbc:oracle:thin:@//{host}:{port}/{service_name}") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("oracle.jdbc.timezoneAsRegion", False) \
        .option("user", user) \
        .option("password", password)


def configure_mitedocta_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    url = """jdbc:oracle:thin:@(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.22.164.19)(PORT = 16161))
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.22.164.20)(PORT = 16161))
      (ADDRESS = (PROTOCOL = TCP)(HOST = 172.22.164.21)(PORT = 16161))
      (LOAD_BALANCE = yes)
    )
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = QA34)
    )
  )"""
    user = os.getenv(")BUC_USER")
    password = os.getenv("BUC_PASSWORD")

    return configure_jdbc_spark(connection, table, reading) \
        .option("url", url) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("oracle.jdbc.timezoneAsRegion", False) \
        .option("user", user) \
        .option("password", password)


def configure_integrity_spark(database: str) -> SparkConnectionConfigurator:
    def creator(connection, table, reading):
        host = os.getenv("INTEGRITY_HOST")
        port = os.getenv("INTEGRITY_PORT")
        user = os.getenv("INTEGRITY_USER")
        password = os.getenv("INTEGRITY_PASSWORD")

        return configure_jdbc_spark(connection, table, reading) \
            .option("url", f"jdbc:rdbThin://{host}:{port}/mexico$base:{database}@transaction=readonly") \
            .option("driver", "oracle.rdb.jdbc.rdbThin.Driver") \
            .option("oracle.jdbc.timezoneAsRegion", False) \
            .option("user", user) \
            .option("password", password)

    return creator


def configure_postgres_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT"))
    database = os.getenv("POSTGRES_DATABASE")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    return configure_jdbc_spark(connection, table, reading) \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("search_path", '"MAESTROS","GESTOR","HECHOS","RESULTADOS","ESTADO_CUENTA"') \
        .option("user", user) \
        .option("password", password)


def configure_bigquery_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    bucket = os.getenv("BIGQUERY_BUCKET")

    if reading:
        connection = connection.option("query", table) \
            .option("viewsEnabled", True)
    else:
        connection = connection.option("table", table)

    return connection \
        .format("bigquery") \
        .option("temporaryGcsBucket", bucket)


def configure_jdbc_spark(connection: SparkConnection, table: str, reading: bool) -> SparkConnection:
    if reading:
        connection = connection \
            .option("numPartitions", 10) \
            .option("fetchsize", 100_000) \
            .option("query", table)
    else:
        connection = connection \
            .option("numPartitions", 20) \
            .option("batchsize", 100_000) \
            .option("dbtable", table)

    return connection .format("jdbc")


def configure_parquet_spark(connection: SparkConnection, filename: str, _: bool) -> SparkConnection:
    return connection \
        .format("parquet") \
        .option("path", filename)


def configure_avro_spark(connection: SparkConnection, filename: str, _: bool) -> SparkConnection:
    return connection \
        .format("avro") \
        .option("path", filename)


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
