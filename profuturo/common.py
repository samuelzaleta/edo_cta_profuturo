from sqlalchemy.engine import Connection, CursorResult, Row
from typing import Dict, List
from sqlalchemy import text
from datetime import datetime, time, timedelta


class MemoryLogger:
    __log: str

    def __init__(self):
        self.__log = ""

    def log(self, message: str) -> None:
        self.__log = self.__log + message + '\n'
        print(message)

    def get_log(self) -> str:
        return self.__log


class HtmlReporter:
    _conn: Connection
    _cursor: CursorResult
    _display_columns: List[str]
    _value_columns: List[str]
    _current_displays: Dict[str, str]
    _current_values: Dict[str, Dict[str, float]]
    _current_totals: Dict[str, float]
    _html: str

    def generate(self, conn: Connection, query: str, display_columns: List[str], value_columns: List[str]) -> str:
        self._conn = conn
        self._cursor = conn.execute(text(query))
        self._display_columns = display_columns
        self._value_columns = value_columns
        self._current_displays = {}
        self._current_values = {}
        self._current_totals = {}
        self._html = ''

        self._append_header()
        self._append_records()
        self._append_footer()

        return self._html

    def _append_header(self) -> None:
        self._append_html("""
        <table>
        <thead>
          <tr style="background-color: #004B8D; color: white;">
        """)

        for column in self._display_columns + self._value_columns:
            self._append_html(f"<th>{column}</th>")

        self._append_html("""
          </tr>
        </thead>
        <tbody>
        """)

    def _append_records(self) -> None:
        for row in self._cursor.fetchall():
            for i, column_display in enumerate(self._display_columns):
                current_display = self._current_displays.get(column_display, row[i])

                if current_display != row[i]:
                    self._append_subgroup(column_display, current_display)

                self._current_displays[column_display] = row[i]

                for j, column_value in enumerate(self._value_columns, len(self._display_columns)):
                    self._current_values[column_display] = self._current_values.get(column_display, {})
                    self._current_values[column_display][column_value] = \
                        self._current_values[column_display].get(column_value, 0) + row[j]

            self._append_row(row)

            for j, column_value in enumerate(self._value_columns, len(self._display_columns)):
                self._current_totals[column_value] = self._current_totals.get(column_value, 0) + row[j]

    def _append_subgroup(self, group: str, current_value: str):
        self._append_html(f"""
        <tr style="background-color: #FFC000; color: black; text-align: right;">
          <td colspan="{len(self._display_columns)}">{group} {current_value}</td>
        """)

        for column_value, current_value in self._current_values[group].items():
            self._append_html(f"<td>{current_value}</td>")
            self._current_values[group][column_value] = 0

        self._append_html("</tr>")

    def _append_row(self, row: Row):
        self._append_html("<tr>")

        for value in row:
            self._append_html(f"<td>{value}</td>")

        self._append_html("</tr>")

    def _append_footer(self) -> None:
        self._append_html(f"""
            <tr style="background-color: black; color: white; text-align: right;">
                <td colspan="{len(self._display_columns)}">TOTAL</td>
        """)

        for total in self._current_totals.values():
            self._append_html(f"<td>{total}</td>")

        self._append_html("""
            </tr>
        </tbody>
        </table>
        """)

    def _append_html(self, html: str):
        self._html = self._html + html


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
