from typing import Union
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


@contextmanager
def use_pools(phase: int, *pools: Engine):
    with ExitStack() as stack:
        conns = []

        for pool in pools:
            try:
                conns.append(stack.enter_context(pool.begin()))
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
    host = os.getenv("INTEGRITY_HOST")
    port = int(os.getenv("INTEGRITY_PORT"))
    user = os.getenv("INTEGRITY_USER")
    password = os.getenv("INTEGRITY_PASSWORD")

    return lambda: jaydebeapi.connect(
        "oracle.rdb.jdbc.rdbThin.Driver",
        f"jdbc:rdbThin://{host}:{port}/mexico$base:{database}",
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
    )


def configure_mit_spark(connection: Union[DataFrameReader, DataFrameWriter]) -> Union[DataFrameReader, DataFrameWriter]:
    host = os.getenv("MIT_HOST")
    port = int(os.getenv("MIT_PORT"))
    service_name = os.getenv("MIT_DATABASE")
    user = os.getenv("MIT_USER")
    password = os.getenv("MIT_PASSWORD")

    return connection \
        .option("url", f"jdbc:oracle:thin:@//{host}:{port}/{service_name}") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("oracle.jdbc.timezoneAsRegion", False) \
        .option("user", user) \
        .option("password", password)


def configure_buc_spark(connection: Union[DataFrameReader, DataFrameWriter]) -> Union[DataFrameReader, DataFrameWriter]:
    host = os.getenv("BUC_HOST")
    port = int(os.getenv("BUC_PORT"))
    service_name = os.getenv("BUC_DATABASE")
    user = os.getenv("BUC_USER")
    password = os.getenv("BUC_PASSWORD")

    return connection \
        .option("url", f"jdbc:oracle:thin:@//{host}:{port}/{service_name}") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("oracle.jdbc.timezoneAsRegion", False) \
        .option("user", user) \
        .option("password", password)


def configure_postgres_spark(connection: Union[DataFrameReader, DataFrameWriter]) -> Union[DataFrameReader, DataFrameWriter]:
    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT"))
    database = os.getenv("POSTGRES_DATABASE")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    return connection \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("search_path", '"MAESTROS","GESTOR","HECHOS","RESULTADOS"') \
        .option("user", user) \
        .option("password", password)


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
