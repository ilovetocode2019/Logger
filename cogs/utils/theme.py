from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    Color: TypeAlias = Tuple[int, int, int]

class Theme:

    def __init__(self, name: str, primary: Color, secondary: Color, background: Color) -> None:
        self.name = name
        self.primary = primary
        self.secondary = secondary
        self.background = background

    def __str__(self) -> str:
        return self.name


DARK = Theme("dark", (255, 255, 255), (185, 185, 185), (54, 57, 63))
LIGHT = Theme("light", (0, 0, 0), (64, 64, 64), (255, 255, 255))

THEME_MAPPING: Dict[Optional[int], Theme] = {
    None: DARK,  # default theme
    0: DARK,
    1: LIGHT
}
THEMES: List[Theme] = [DARK, LIGHT]


def get_theme(theme_id: Optional[int]) -> Theme:
    return THEME_MAPPING[theme_id]
