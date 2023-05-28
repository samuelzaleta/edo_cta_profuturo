from sqlalchemy import text
import sqlalchemy
import oracledb
import psycopg2


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
        password="Alexo123",
        database="postgres",
        options="-c search_path=maestros,gestor,hechos",
    )


def get_mit_pool():
    oracledb.init_oracle_client()

    mit_pool = sqlalchemy.create_engine(
        "oracle+oracledb://",
        creator=get_mit_conn,
    )

    return mit_pool


def get_buc_pool():
    buc_pool = sqlalchemy.create_engine(
        "oracle+oracledb://",
        creator=get_buc_conn,
    )

    return buc_pool


def get_postgres_pool():
    postgres_pool = sqlalchemy.create_engine(
        "postgresql+psycopg2://",
        creator=get_postgres_conn,
    )

    return postgres_pool
