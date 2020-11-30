class Theme:
    def __init__(self, name, primary, secondary, background):
        self.name = name
        self.primary = primary
        self.secondary = secondary
        self.background = background

    def __str__(self):
        return self.name


DARK = Theme("dark", (255, 255, 255), (185, 185, 185), (54, 57, 63))
LIGHT = Theme("light", (0, 0, 0), (64, 64, 64), (255, 255, 255))

THEME_MAPPING = {
    None: LIGHT,  # default theme
    0: DARK,
    1: LIGHT
}


def get_theme(theme):
    return THEME_MAPPING[theme]
