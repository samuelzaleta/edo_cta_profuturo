from sqlalchemy.exc import OperationalError
from sqlalchemy import Engine
from contextlib import ExitStack, contextmanager
from .exceptions import ProfuturoException
import jaydebeapi
import sqlalchemy
import oracledb
import psycopg2


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
        host="172.22.180.190",
        service_name="mitafore.profuturo-gnp.net",
        user="PROFUTURO_QAMOD",
        password="Pa55w0rd*19",
    )


def get_buc_conn() -> oracledb.Connection:
    return oracledb.connect(
        host="172.22.164.19",
        port=16161,
        service_name="QA34",
        user="CLUNICO",
        password="temp4now13",
    )


def get_postgres_conn():
    return psycopg2.connect(
        host="34.72.193.129",
        user="alexo",
        password="Oxela3210",
        database="PROFUTURO",
        port=5432,
        options='-c search_path="MAESTROS","GESTOR","HECHOS","RESULTADOS"',
    )


def get_integrity_conn(database: str):
    return lambda: jaydebeapi.connect(
        "oracle.rdb.jdbc.rdbThin.Driver",
        f"jdbc:rdbThin://130.40.30.144:1714/mexico$base:{database}",
        {"user": "SIEFORE", "password": "SIEFORE2019"},
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


def get_mit_url():
    user = "PROFUTURO_QAMOD"
    password = "Pa55w0rd*19"
    host = "172.22.180.190"
    port = '1521'
    service_name = "mitafore.profuturo-gnp.net"
    
    return f"oracle://{user}:{password}@{host}:{port}/{service_name}"


def get_buc_url():
    user = "CLUNICO"
    password = "temp4now13"
    host = "172.22.164.19"
    port = '16161'
    service_name = "QA34"
    
    return f"oracle://{user}:{password}@{host}:{port}/{service_name}"


def get_postgres_url():
    host = "34.72.193.129"
    user = "alexo"
    password = "Oxela3210"
    database = "PROFUTURO"
    port = '5432'
    
    return f'postgresql://{user}:{password}@{host}:{port}/{database}'
