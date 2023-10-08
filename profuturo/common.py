from sqlalchemy import text, Engine, Connection
from datetime import datetime, time, timedelta
from contextlib import contextmanager
from dotenv import load_dotenv
from typing import Union
from .database import use_pools
from .exceptions import ProfuturoException


@contextmanager
def define_extraction(phase: int, area: int, main_pool: Engine, *pools: Engine):
    load_dotenv()

    with notify_exceptions(main_pool, phase, area):
        with use_pools(phase, main_pool, *pools) as pools:
            yield pools


@contextmanager
def register_time(pool: Engine, phase: int, term: int, user: int, area: int):
    with pool.begin() as conn:
        binnacle_id = register_start(conn, phase, term, user, area, datetime.now())

    exc = None
    try:
        yield
    except Exception as e:
        exc = e

    with pool.begin() as conn:
        register_end(conn, binnacle_id, datetime.now(), exc)


def register_start(conn: Connection, phase: int, term: int, user: int, area: int, start: datetime) -> int:
    try:
        cursor = conn.execute(text("""
        INSERT INTO "TTGESPRO_BITACORA_ESTADO_CUENTA" (
           "FTD_FECHA_HORA_INICIO", "FCN_ID_PERIODO", "FCN_ID_FASE", "FCN_ID_AREA", "FCN_ID_USUARIO"
        )
        VALUES (:start, :term, :phase, :area, :user)
        RETURNING "FTN_ID_BITACORA_ESTADO_CUENTA"
        """), {
            "start": start,
            "phase": phase,
            "term": term,
            "user": user,
            "area": area,
        })
        row = cursor.fetchone()

        return row[0]
    except Exception as e:
        raise ProfuturoException("BINNACLE_ERROR", phase, term) from e


def register_end(conn: Connection, binnacle_id: int, end: datetime, exc: Union[Exception, None]) -> None:
    if exc:
        conn.execute(text("""
        UPDATE "TTGESPRO_BITACORA_ESTADO_CUENTA"
        SET "FTD_FECHA_HORA_FIN" = :end, "FTC_BANDERA_NIVEL_SERVICIO" = :flag
        WHERE "FTN_ID_BITACORA_ESTADO_CUENTA" = :id
        """), {"id": binnacle_id, "end": end, "flag": "Error"})

        raise exc

    try:
        cursor = conn.execute(text("""
        SELECT "FTD_FECHA_HORA_INICIO", "FCN_ID_FASE"
        FROM "TTGESPRO_BITACORA_ESTADO_CUENTA"
        WHERE "FTN_ID_BITACORA_ESTADO_CUENTA" = :id
        """), {'id': binnacle_id})

        binnacle = cursor.fetchone()
        start = binnacle[0]
        phase = binnacle[1]

        cursor = conn.execute(text("""
        SELECT "FTC_VERDE", "FTC_AMARILLO"
        FROM "TCGESPRO_FASE"
        WHERE "FTN_ID_FASE" = :phase
        """), {'phase': phase})

        if cursor.rowcount == 0:
            raise ValueError('Phase not found', phase)

        delta = end - start
        service_level = cursor.fetchone()
        green_time = time.fromisoformat(service_level[0])
        yellow_time = time.fromisoformat(service_level[1])

        flag: str
        if delta <= timedelta(hours=green_time.hour, minutes=green_time.minute):
            flag = "Verde"
        elif delta <= timedelta(hours=yellow_time.hour, minutes=yellow_time.minute):
            flag = "Amarillo"
        else:
            flag = "Rojo"

        conn.execute(text("""
        UPDATE "TTGESPRO_BITACORA_ESTADO_CUENTA"
        SET "FTD_FECHA_HORA_FIN" = :end, "FTC_BANDERA_NIVEL_SERVICIO" = :flag
        WHERE "FTN_ID_BITACORA_ESTADO_CUENTA" = :id
        """), {"id": binnacle_id, "end": end, "flag": flag})
    except Exception as e:
        raise ProfuturoException("BINNACLE_ERROR") from e


@contextmanager
def notify_exceptions(pool: Engine, phase: int, area: int):
    try:
        yield
    except ProfuturoException as e:
        with pool.begin() as conn:
            notify(
                conn,
                f"Error al ingestar la etapa {phase}",
                phase,
                area,
                term=e.term,
                message=e.msg,
                details=str(e),
                control=False,
            )

        raise e
    except Exception as e:
        with pool.begin() as conn:
            notify(
                conn,
                f"Error desconocido al ingestar la etapa {phase}",
                phase,
                area,
                details=str(e),
                control=False,
            )

        raise e


def notify(
    conn: Connection,
    title: str,
    phase: int,
    area: int,
    term: int = None,
    message: str = None,
    details: str = None,
    control: bool = True,
    validated: bool = False,
) -> None:
    users = conn.execute(text(f"""
    SELECT DISTINCT "FCN_ID_USUARIO"
    FROM "MAESTROS"."TCDATMAE_AREA" a
        INNER JOIN "GESTOR"."TCGESPRO_ROL_AREA" ra ON a."FTN_ID_AREA" = ra."FCN_ID_AREA"
        INNER JOIN "GESTOR"."TCGESPRO_ROL" rp ON ra."FCN_ID_ROL" =  rp."FTN_ID_ROL"
        INNER JOIN "GESTOR"."TCGESPRO_ROL_MENU_PRIVILEGIO" rmp ON rp."FTN_ID_ROL" = rmp."FCN_ID_ROL"
        INNER JOIN "GESTOR"."TTGESPRO_ROL_USUARIO" ru ON ra."FCN_ID_ROL" = ru."FCN_ID_ROL"
        INNER JOIN "GESTOR"."TCGESPRO_MENU_FUNCION" mf ON rmp."FCN_ID_MENU" =  mf."FTN_ID_FUNCION"
    WHERE "FTN_ID_AREA" = :area
      AND "FTN_ID_MENU" = 7
    """), {"area": area})
    if users.rowcount == 0:
        raise ValueError("The area does not exist")

    notification = conn.execute(text("""
    INSERT INTO "TTGESPRO_NOTIFICACION" (
        "FTC_TITULO", "FTC_DETALLE_TEXTO", "FTC_DETALLE_BLOB", 
        "FTB_CIFRAS_CONTROL", "FCN_ID_PERIODO", "FCN_ID_FASE",
        "FTB_CIFRAS_CONTROL_VALIDADAS", "FTD_FECHA_CREACION"
    )
    VALUES (:title, :message, :details, :control, :term, :phase, :validated, now())
    RETURNING "FTN_ID_NOTIFICACION"
    """), {
        "title": title,
        "phase": phase,
        "term": term,
        "message": message,
        "details": details,
        "control": control,
        "validated": validated,
    }).fetchone()

    for user in users.fetchall():
        conn.execute(text("""
        INSERT INTO "TTGESPRO_NOTIFICACION_USUARIO" ("FCN_ID_NOTIFICACION", "FCN_ID_USUARIO")
        VALUES (:notification, :user)
        """), {
            "notification": notification[0],
            "user": user[0]
        })


def truncate_table(conn: Connection, table: str, term: int = None) -> None:
    try:
        print(f"Truncating {table}...")
        if term:
            conn.execute(text(f'DELETE FROM "{table}" WHERE "FCN_ID_PERIODO" = :term'), {"term": term})
        else:
            conn.execute(text(f'TRUNCATE TABLE "{table}"'))
        print(f"Truncated {table}!")
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e

