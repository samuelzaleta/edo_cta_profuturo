from py4j.java_gateway import java_import
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text, delete, column, table, literal
from datetime import time, timedelta
from contextlib import contextmanager
from typing import Union
from .database import use_pools
from .env import load_env
from .exceptions import ProfuturoException
from .extraction import _get_spark_session


@contextmanager
def define_extraction(phase: int, area: int, main_pool: Engine, *pools: Engine):
    spark = _get_spark_session()
    gw = spark.sparkContext._gateway

    load_env()
    java_import(gw.jvm, "org.profuturo.rdb.RdbDialect")
    gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(gw.jvm.org.profuturo.rdb.RdbDialect())

    with notify_exceptions(main_pool, phase, area):
        with use_pools(phase, main_pool, *pools) as pools:
            yield pools

    gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.unregisterDialect(gw.jvm.org.profuturo.rdb.RdbDialect())


@contextmanager
def register_time(pool: Engine, phase: int, term: int, user: int, area: int):
    with pool.begin() as conn:
        binnacle_id = register_start(conn, phase, term, user, area)

    exc = None
    try:
        yield
    except Exception as e:
        exc = e

    with pool.begin() as conn:
        register_end(conn, binnacle_id, exc)


def register_start(conn: Connection, phase: int, term: int, user: int, area: int) -> int:
    try:
        cursor = conn.execute(text("""
        INSERT INTO "TTGESPRO_BITACORA_ESTADO_CUENTA" (
           "FTD_FECHA_HORA_INICIO", "FCN_ID_PERIODO", "FCN_ID_FASE", "FCN_ID_AREA", "FCN_ID_USUARIO"
        )
        VALUES (now(), :term, :phase, :area, :user)
        RETURNING "FTN_ID_BITACORA_ESTADO_CUENTA"
        """), {
            "phase": phase,
            "term": term,
            "user": user,
            "area": area,
        })
        row = cursor.fetchone()

        return row[0]
    except Exception as e:
        raise ProfuturoException("BINNACLE_ERROR", phase, term) from e


def register_end(conn: Connection, binnacle_id: int, exc: Union[Exception, None]) -> None:
    if exc:
        conn.execute(text("""
        UPDATE "TTGESPRO_BITACORA_ESTADO_CUENTA"
        SET "FTD_FECHA_HORA_FIN" = now(), "FTC_BANDERA_NIVEL_SERVICIO" = :flag
        WHERE "FTN_ID_BITACORA_ESTADO_CUENTA" = :id
        """), {"id": binnacle_id, "flag": "Error"})

        raise exc

    try:
        cursor = conn.execute(text("""
        UPDATE "TTGESPRO_BITACORA_ESTADO_CUENTA"
        SET "FTD_FECHA_HORA_FIN" = now()
        WHERE "FTN_ID_BITACORA_ESTADO_CUENTA" = :id
        RETURNING "FTD_FECHA_HORA_INICIO", "FTD_FECHA_HORA_FIN", "FCN_ID_FASE"
        """), {"id": binnacle_id})

        binnacle = cursor.fetchone()
        start = binnacle[0]
        end = binnacle[1]
        phase = binnacle[2]

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
        SET "FTC_BANDERA_NIVEL_SERVICIO" = :flag
        WHERE "FTN_ID_BITACORA_ESTADO_CUENTA" = :id
        """), {"id": binnacle_id, "flag": flag})
    except Exception as e:
        raise ProfuturoException("BINNACLE_ERROR") from e


@contextmanager
def notify_exceptions(pool: Engine, phase: int, area: int):
    try:
        yield
    except ProfuturoException as e:
        with pool.begin() as conn:
            cursor = conn.execute(text("""
                    SELECT "FTC_DESCRIPCION_CORTA"
                    FROM "TCGESPRO_FASE"
                    WHERE "FTN_ID_FASE" = :phase
                    """), {'phase': phase})
            row = cursor.fetchone()
            notify(
                conn,
                f"Error al ingestar la fase {row[0]}",
                phase,
                area,
                term=e.term,
                message=e.msg,
                details=str(e),
                control=False,
                aprobar=False,
                descarga=False
            )

        raise e
    except Exception as e:
        with pool.begin() as conn:
            cursor = conn.execute(text("""
                                SELECT "FTC_DESCRIPCION_CORTA"
                                FROM "TCGESPRO_FASE"
                                WHERE "FTN_ID_FASE" = :phase
                                """), {'phase': phase})
            row = cursor.fetchone()
            notify(
                conn,
                f"Error desconocido al ingestar la fase {row[0]}",
                phase,
                area,
                details=str(e),
                control=False,
                aprobar=False,
                descarga=False
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
    descarga: bool = True,
    aprobar: bool = True,
    visualiza: bool = True,
    reproceso: bool = True
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
        "FTB_CIFRAS_CONTROL_VALIDADAS", "FTD_FECHA_CREACION",
        "FTB_APROBAR", "FTB_VISUALIZA_CIFRAS", "FTB_REPROCESO",
        "FTB_DESCARGA", "FCN_ID_AREA"
    )
    VALUES (:title, :message, :details, :control, :term, :phase, :validated, now(), :aprobar, :visualiza, :reproceso, :descarga, :area)
    RETURNING "FTN_ID_NOTIFICACION"
    """), {
        "title": title,
        "phase": phase,
        "term": term,
        "message": message,
        "details": details,
        "control": control,
        "validated": validated,
        "aprobar": aprobar,
        "visualiza": visualiza,
        "reproceso": reproceso,
        "descarga": descarga,
        "area": area,
    }).fetchone()

    for user in users.fetchall():
        conn.execute(text("""
        INSERT INTO "TTGESPRO_NOTIFICACION_USUARIO" ("FCN_ID_NOTIFICACION", "FCN_ID_USUARIO")
        VALUES (:notification, :user)
        """), {
            "notification": notification[0],
            "user": user[0]
        })


def truncate_table(conn: Connection, table_name: str, term: int = None, area: int = None) -> None:
    try:
        print(f"Truncating {table_name}...")

        statement = delete(table(table_name)).where(literal(1) == literal(1))
        if term:
            statement = statement.where(column("FCN_ID_PERIODO") == literal(term))
        if area:
            statement = statement.where(column("FCN_ID_AREA") == literal(area))
        conn.execute(statement)

        print(f"Truncated {table_name}!")
    except Exception as e:
        raise ProfuturoException.from_exception(e, term) from e
