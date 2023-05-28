from sqlalchemy.engine import Connection
from sqlalchemy import text
from typing import Dict, List, Any
from datetime import datetime, date
from .common import write_binnacle, notify
from ._helpers import group_by, chunk
import calendar
import pandas as pd


def extract_terms(conn: Connection) -> List[Dict[str, Any]]:
    cursor = conn.execute(text("""
    SELECT ftn_id_periodo, ftc_periodo
    FROM tcgespro_periodos
    """))
    terms = []

    for row in cursor.fetchall():
        term = row[1].split('-')
        year = int(term[0])
        month = int(term[1])

        month_range = calendar.monthrange(year, month)
        start_month = date(year, month, 1)
        end_month = date(year, month, month_range[1])

        terms.append({"id": row[0], "start_month": start_month, "end_month": end_month})
        print(f"Extracting period: from {start_month} to {end_month}")

    return terms


def extract_indicator(
    origin: Connection,
    destination: Connection,
    query: str,
    index: int,
    phase: int,
    params: Dict[str, Any] = None,
    limit: int = None,
):
    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print("Extracting fixed indicator...")

    start = datetime.now()

    cursor = origin.execute(text(query), params)
    for value, accounts in group_by(cursor.fetchall(), lambda row: row[1], lambda row: row[0]).items():
        for i, batch in enumerate(chunk(accounts, 1_000)):
            destination.execute(text("""
            UPDATE tcdatmae_clientes
            SET FTA_INDICADORES[:index] = :value
            WHERE FTN_CUENTA IN :accounts
            """), {
                "accounts": tuple(batch),
                "index": index,
                "value": value,
            })

            print(f"Updating records {i * 1_000} throught {(i + 1) * 1_000}")

    end = datetime.now()

    write_binnacle(destination, phase, start, end)

    print("Done extracting fixed indicator!")


def extract_dataset(
    origin: Connection,
    destination: Connection,
    query: str,
    table: str,
    phase: int,
    term: int = None,
    params: Dict[str, Any] = None,
    limit: int = None,
):
    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Extracting {table}...")

    start = datetime.now()

    try:
        df_pd = pd.read_sql_query(text(query), origin, params=params)

        if term:
            df_pd = df_pd.assign(fcn_id_periodo=term)

        df_pd.to_sql(
            table,
            destination,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1_000,
        )
    except Exception as e:
        notify(
            destination,
            f"Error al ingestar {table}",
            f"Hubo un error al ingestar {table}",
            f"Mensaje de error: {e}",
            term,
        )
        raise e

    end = datetime.now()

    write_binnacle(destination, phase, start, end, term)

    print(f"Done extracting {table}!")
    print(df_pd.info())
