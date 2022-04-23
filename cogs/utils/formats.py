from __future__ import annotations

from typing import Any, List


class plural:
    def __init__(self, value: int) -> None:
        self.value = value

    def __format__(self, format_spec: str) -> str:
        if self.value == 1:
            return f"{self.value} {format_spec}"
        else:
            return f"{self.value} {format_spec}s"

class Tabulate:
    def __init__(self) -> None:
        self.widths: List[int] = []
        self.columns: List[str] = []
        self.rows: List[List[str]] = []

    def add_column(self, column: str) -> None:
        self.columns.append(column)
        self.widths.append(len(column) + 2)

    def add_columns(self, columns: List[str]) -> None:
        for column in columns:
            self.add_column(column)

    def add_row(self, row: List[Any]) -> None:
        values = [str(value) for value in row]
        self.rows.append(values)
        for counter, value in enumerate(values):
            width = len(value)+2
            if width > self.widths[counter]:
                self.widths[counter] = width

    def add_rows(self, rows: List[List[Any]]):
        for row in rows:
            self.add_row(row)

    def draw_row(self, row: List[str]) -> str:
        drawing = "║".join([f"{value:^{self.widths[counter]}}" for counter, value in enumerate(row)])
        return f"║{drawing}║"

    def draw(self) -> str:
        top = "╦".join(["═"*width for width in self.widths])
        top = f"╔{top}╗"

        bottom = "╩".join(["═"*width for width in self.widths])
        bottom = f"╚{bottom}╝"

        seperator = "╬".join(["═"*width for width in self.widths])
        seperator = f"║{seperator}║"

        drawing = [top]
        drawing.append(self.draw_row(self.columns))
        drawing.append(seperator)

        for row in self.rows:
            drawing.append(self.draw_row(row))
        drawing.append(bottom)

        return "\n".join(drawing)

    def __str__(self) -> str:
        return self.draw()

    def __repr__(self) -> str:
        return self.draw()
