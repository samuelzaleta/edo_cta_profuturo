from sqlalchemy.engine import Connection
from sqlalchemy import text, Engine
from datetime import datetime, time, timedelta
from .database import use_pools
from .exceptions import ProfuturoException
import contextlib


@contextlib.contextmanager
def define_extraction(phase: int, main_pool: Engine, *pools: Engine):
    with notify_exceptions(main_pool, phase):
        with use_pools(phase, main_pool, *pools) as pools:
            yield pools


@contextlib.contextmanager
def register_time(conn: Connection, phase: int, term: int = None):
    start = datetime.now()
    yield
    end = datetime.now()

    write_binnacle(conn, phase, start, end, term)


def write_binnacle(conn: Connection, phase: int, start: datetime, end: datetime, term: int = None) -> None:
    try:
        cursor = conn.execute(text("""
        SELECT "FTC_VERDE", "FTC_AMARILLO"
        FROM "TCGESPRO_FASE"
        WHERE "FTN_ID_FASE" = :phase
        """), {'phase': phase})

        if cursor.rowcount != 1:
            print(f'Phase {phase} not found')
            return

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
        INSERT INTO "TTGESPRO_BITACORA_ESTADO_CUENTA" (
            "FTD_FECHA_HORA_INICIO", "FTD_FECHA_HORA_FIN", "FTC_BANDERA_NIVEL_SERVICIO", 
            "FCN_ID_PERIODO", "FCN_ID_FASE"
        ) 
        VALUES (:start, :end, :flag, :term, :phase)
        """), {
            "start": start,
            "end": end,
            "flag": flag,
            "term": term,
            "phase": phase,
        })
    except Exception as e:
        raise ProfuturoException("BINNACLE_ERROR", phase, term) from e


@contextlib.contextmanager
def notify_exceptions(pool: Engine, phase: int):
    try:
        yield
    except ProfuturoException as e:
        with pool.begin() as conn:
            notify(
                conn,
                f"Error al ingestar la etapa {phase}",
                e.msg,
                str(e),
                e.term,
            )

        raise e
    except Exception as e:
        with pool.begin() as conn:
            notify(conn, f"Error desconocido al ingestar la etapa {phase}", details=str(e))

        raise e


def notify(conn: Connection, title: str, message: str = None, details: str = None, term: int = None, control: bool = False):
    conn.execute(text("""
    INSERT INTO "TTGESPRO_NOTIFICACION" (
        "FTC_TITULO", "FTC_DETALLE_TEXTO", "FTC_DETALLE_BLOB", 
        "FTB_CIFRAS_CONTROL", "FCN_ID_PERIODO", "FCN_ID_USUARIO"
    )
    VALUES (:title, :message, :details, :control, :term, 0)
    """), {
        "title": title,
        "message": message,
        "details": details,
        "control": control,
        "term": term,
    })


def truncate_table(conn: Connection, table: str, term: int = None) -> None:
    try:
        if term:
            conn.execute(text(f'DELETE FROM "{table}" WHERE "FCN_ID_PERIODO" = :term'), {"term": term})
        else:
            conn.execute(text(f'TRUNCATE "{table}"'))
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e
