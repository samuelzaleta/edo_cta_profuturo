from sqlalchemy import text, Connection
from typing import Dict, List, Any
from datetime import date
from .exceptions import ProfuturoException
from ._helpers import group_by, chunk
import calendar
import pandas as pd


def extract_terms(conn: Connection, phase: int) -> List[Dict[str, Any]]:
    try:
        cursor = conn.execute(text("""
        SELECT ftn_id_periodo, ftc_periodo
        FROM tcgespro_periodos
        """))
        terms = []

        if cursor.rowcount == 0:
            raise ValueError("The terms table should have at least one term", phase)

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
    except Exception as e:
        raise ProfuturoException("TERMS_ERROR", phase) from e


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

    cursor = origin.execute(text(query), params)
    for value, accounts in group_by(cursor.fetchall(), lambda row: row[1], lambda row: row[0]).items():
        for i, batch in enumerate(chunk(accounts, 1_000)):
            destination.execute(text("""
            UPDATE tcdatmae_clientes
            SET FTO_INDICADORES = jsonb_set(FTO_INDICADORES, :field, :value)
            WHERE FTN_CUENTA IN :accounts
            """), {
                "accounts": tuple(batch),
                "field": f"{{{index}}}",
                "value": f'"{value}"',
            })

            print(f"Updating records {i * 1_000} throught {(i + 1) * 1_000}")


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
        raise ProfuturoException("UNKNOWN_ERROR", phase, term) from e

    print(f"Done extracting {table}!")
    print(df_pd.info())
