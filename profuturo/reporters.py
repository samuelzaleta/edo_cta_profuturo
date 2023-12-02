from decimal import Decimal
from sqlalchemy.engine import Connection, CursorResult, Row
from sqlalchemy import text
from typing import Dict, List, Any, Union


def _format_value(value: Decimal, positions: int) -> str:
    return str(value.quantize(Decimal(10) ** -positions))


class HtmlReporter:
    _conn: Connection
    _cursor: CursorResult
    _display_columns: List[str]
    _value_columns: List[str]
    _precision_columns: Dict[str, int]
    _current_totals: Dict[str, Decimal]
    _html: str

    def generate(
        self,
        conn: Connection,
        query: str,
        display_columns: List[str],
        value_columns: Union[List[str], Dict[str, int]],
        params: Dict[str, Any] = None,
    ) -> str:
        if not params:
            params = {}

        self._conn = conn
        self._cursor = conn.execute(text(query), params)
        self._display_columns = display_columns
        if isinstance(value_columns, dict):
            self._value_columns = list(value_columns)
            self._precision_columns = value_columns
        else:
            self._value_columns = value_columns
            self._precision_columns = {}.fromkeys(value_columns, 2)
        self._current_totals = {}
        self._html = ''

        self._append_header()
        self._append_records()
        self._append_footer()

        return self._html

    def _append_header(self) -> None:
        self._html += """
        <table>
        <thead>
          <tr style="background-color: #004B8D; color: white;">
        """

        for column in self._display_columns + self._value_columns:
            self._html += f"<th>{column}</th>"

        self._html += """
          </tr>
        </thead>
        <tbody>
        """

    def _append_records(self) -> None:
        for row in self._cursor.fetchall():
            self._append_row(row)

            for j, column_value in enumerate(self._value_columns, len(self._display_columns)):
                self._current_totals[column_value] = Decimal(self._current_totals.get(column_value, 0)) + Decimal(row[j] or 0)

    def _append_row(self, row: Row):
        self._html += "<tr>"

        if isinstance(self._value_columns, dict):
            value_columns = self._value_columns.keys()
        else:
            value_columns = self._value_columns

        i = 0
        for _ in self._display_columns:
            self._html += f"<td>{row[i]}</td>"
            i += 1
        for column in value_columns:
            precision = self._precision_columns[column]

            self._html += f'<td style="text-align: right;">{_format_value(Decimal(row[i] or 0), precision)}</td>'
            i += 1

        self._html += "</tr>"

    def _append_footer(self) -> None:
        self._html += f"""
            <tr style="background-color: black; color: white; text-align: right;">
                <td colspan="{len(self._display_columns)}">TOTAL</td>
        """

        for column, total in self._current_totals.items():
            precision = self._precision_columns[column]

            self._html += f"<td>{_format_value(total, precision)}</td>"

        self._html += """
            </tr>
        </tbody>
        </table>
        """
