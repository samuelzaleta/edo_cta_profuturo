from sqlalchemy.engine import Connection, CursorResult, Row
from sqlalchemy import text
from typing import Dict, List, Any


def _format_value(value: float) -> str:
    return '{:,.2f}'.format(round(value, 2))


class HtmlReporter:
    _conn: Connection
    _cursor: CursorResult
    _display_columns: List[str]
    _value_columns: List[str]
    _current_displays: Dict[str, str]
    _current_values: Dict[str, Dict[str, float]]
    _current_totals: Dict[str, float]
    _html: str

    def generate(
        self,
        conn: Connection,
        query: str,
        display_columns: List[str],
        value_columns: List[str],
        params: Dict[str, Any] = None,
    ) -> str:
        if not params:
            params = {}

        self._conn = conn
        self._cursor = conn.execute(text(query), params)
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
            self._append_html(f"<td>{_format_value(current_value)}</td>")
            self._current_values[group][column_value] = 0

        self._append_html("</tr>")

    def _append_row(self, row: Row):
        self._append_html("<tr>")

        i = 0
        for _ in self._display_columns:
            self._append_html(f"<td>{row[i]}</td>")
            i = i + 1
        for _ in self._value_columns:
            self._append_html(f'<td style="text-align: right;">{_format_value(row[i])}</td>')
            i = i + 1

        self._append_html("</tr>")

    def _append_footer(self) -> None:
        self._append_html(f"""
            <tr style="background-color: black; color: white; text-align: right;">
                <td colspan="{len(self._display_columns)}">TOTAL</td>
        """)

        for total in self._current_totals.values():
            self._append_html(f"<td>{_format_value(total)}</td>")

        self._append_html("""
            </tr>
        </tbody>
        </table>
        """)

    def _append_html(self, html: str):
        self._html = self._html + html
