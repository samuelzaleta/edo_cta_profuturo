from sqlalchemy import text, Engine, Connection
from datetime import datetime, time, timedelta
from contextlib import contextmanager
from dotenv import load_dotenv
from .database import use_pools
from .exceptions import ProfuturoException


@contextmanager
def define_extraction(phase: int, main_pool: Engine, *pools: Engine):
    load_dotenv()

    with notify_exceptions(main_pool, phase):
        with use_pools(phase, main_pool, *pools) as pools:
            yield pools


@contextmanager
def register_time(pool: Engine, phase: int, area: int, usuario: int, term: int = None):
    with pool.begin() as conn:
        binnacle_id = register_start(conn, phase, area, usuario, datetime.now(), term=term)

    yield

    with pool.begin() as conn:
        register_end(conn, binnacle_id, datetime.now())


def register_start(conn: Connection, phase: int, area: int, usuario: int ,start: datetime, term: int = None) -> int:
    try:
        cursor = conn.execute(text("""
        INSERT INTO "TTGESPRO_BITACORA_ESTADO_CUENTA" (
           "FTD_FECHA_HORA_INICIO", "FCN_ID_PERIODO", "FCN_ID_FASE", "FCN_ID_AREA", "FCN_ID_USUARIO")
        VALUES (:start, :term, :phase, :area, :usuario)
        RETURNING "FTN_ID_BITACORA_ESTADO_CUENTA"
        """), {
            "start": start,
            "term": term,
            "phase": phase,
            "area": area,
            "usuario": usuario
        })
        row = cursor.fetchone()

        return row[0]
    except Exception as e:
        raise ProfuturoException("BINNACLE_ERROR", phase, term) from e


def register_end(conn: Connection, binnacle_id: int, end: datetime) -> None:
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


def notify(conn: Connection, title: str, message: str = None, details: str = None, term: int = None,
           control: bool = True, area: int = 0, fase: int = None,
           control_validadas: bool = False):
    cursor = conn.execute(text(f"""
    select
        DISTINCT "FCN_ID_USUARIO"
    from "MAESTROS"."TCDATMAE_AREA" ma
    INNER JOIN "GESTOR"."TCGESPRO_ROL_AREA" ra
    ON ma."FTN_ID_AREA" = ra."FCN_ID_AREA"
    INNER JOIN "GESTOR"."TCGESPRO_ROL" rp
    ON ra."FCN_ID_ROL" =  rp."FTN_ID_ROL"
    inner join "GESTOR"."TCGESPRO_ROL_MENU_PRIVILEGIO" rmp
    on rp."FTN_ID_ROL" = rmp."FCN_ID_ROL"
    inner join "GESTOR"."TTGESPRO_ROL_USUARIO" ru
    on ra."FCN_ID_ROL" = ru."FCN_ID_ROL"
    INNER JOIN "GESTOR"."TCGESPRO_MENU_FUNCION" mf
    on rmp."FCN_ID_MENU" =  mf."FTN_ID_FUNCION"
    where
    "FTN_ID_AREA" = {area}
    and "FTN_ID_MENU" = 7
    """))
    if cursor.rowcount == 0:
        raise ValueError("The area does not exist")
    for row in cursor.fetchall():
        conn.execute(text("""
        INSERT INTO "TTGESPRO_NOTIFICACION" (
            "FTC_TITULO", "FTC_DETALLE_TEXTO", "FTC_DETALLE_BLOB", 
            "FTB_CIFRAS_CONTROL", "FCN_ID_PERIODO", "FCN_ID_USUARIO", "FCN_ID_FASE",
            "FTB_CIFRAS_CONTROL_VALIDADAS", "FTD_FECHA_CREACION"
        )
        VALUES (:title, :message, :details, :control, :term, :user, :fase, :control_validadas, now())
        """), {
            "title": title,
            "message": message,
            "details": details,
            "control": control,
            "term": term,
            "user": row[0],
            "fase": fase,
            "control_validadas": control_validadas
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

