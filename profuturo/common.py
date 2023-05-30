from sqlalchemy.engine import Connection
from sqlalchemy import text, Engine
from datetime import datetime, time, timedelta
from .database import use_pools
from .exceptions import ProfuturoException
import contextlib


@contextlib.contextmanager
def define_extraction(phase: int, main_pool: Engine, *pools: Engine):
    with notify_exceptions(main_pool):
        with use_pools(phase, main_pool, *pools) as pools:
            yield pools


@contextlib.contextmanager
def register_time(conn: Connection, phase: int, term: int = None):
    start = datetime.now()
    yield
    end = datetime.now()

    write_binnacle(conn, phase, start, end, term)


def write_binnacle(conn: Connection, phase: int, start: datetime, end: datetime, term: int = None) -> None:
    cursor = conn.execute(text("""
    SELECT ftc_verde, ftc_amarillo
    FROM tcgespro_fases
    WHERE ftn_id_fase = :phase
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
    INSERT INTO ttgespro_bitacora_estado_cuenta (fecha_hora_inicio, fecha_hora_final, bandera_nivel_servicio, id_formato_estado_cuenta, id_periodo, id_fase) 
    VALUES (:start, :end, :flag, :format, :term, :phase)
    """), {
        "start": start,
        "end": end,
        "flag": flag,
        "format": 0,
        "term": term,
        "phase": phase,
    })


@contextlib.contextmanager
def notify_exceptions(pool: Engine):
    try:
        yield
    except ProfuturoException as e:
        with pool.begin() as conn:
            notify(
                conn,
                f"Error al ingestar la etapa {e.phase}",
                e.msg,
                str(e),
                e.term,
            )

        raise e


def notify(conn: Connection, title: str, message: str = None, details: str = None, term: int = None, control: bool = False):
    conn.execute(text("""
    INSERT INTO ttgespro_notificacion (
        FTC_TITULO, FTC_DETALLE_TEXTO, FTC_DETALLE_BLOB, 
        FTB_CIFRAS_CONTROL, FCN_ID_PERIODO, FCN_ID_USUARIO
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
    if term:
        conn.execute(text(f"DELETE FROM {table} WHERE FCN_ID_PERIODO = :term"), {"term": term})
    else:
        conn.execute(text(f"TRUNCATE {table}"))
