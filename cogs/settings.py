from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from .utils import cache, theme as theme_module
from .utils.context import Context

if TYPE_CHECKING:
    from typing_extensions import Self, TypeAlias

    from bot import Logger

ThemeConverterRet: TypeAlias = Tuple[theme_module.Theme, Optional[int]]

class UserConfig:
    id: str
    theme: theme_module.Theme

    @classmethod
    def from_record(cls, record: Any) -> Self:
        self = cls()

        self.id = record["id"]
        self.theme = theme_module.get_theme(record["theme"])

        return self


class ThemeConverter(commands.Converter[ThemeConverterRet]):
    async def convert(self, ctx: Context, arg: str) -> ThemeConverterRet:
        arg = arg.lower()

        for k, v in theme_module.THEME_MAPPING.items():
            if str(v) == arg:
                return v, k

        raise commands.BadArgument("Invalid theme provided")


class Settings(commands.Cog):
    """Commands to configure the bot"""

    def __init__(self, bot: Logger) -> None:
        self.bot = bot

    @cache.cache()
    async def fetch_config(self, user_id: int) -> Optional[UserConfig]:
        query = """SELECT *
                   FROM user_config
                   WHERE id=$1;
                """

        record = await self.bot.db.fetchrow(query, user_id)

        if not record:
            return None

        return UserConfig.from_record(record)

    @commands.hybrid_command(name="theme")
    async def theme(self, ctx: Context, *, theme: ThemeConverter = None):  # type: ignore
        """View or change your theme

        Available themes:
         - light (default): The basic discord light theme
         - dark: The basic discord dark theme
        """
        if not theme:
            config = await self.fetch_config(ctx.author.id)
            theme = config.theme if config else theme_module.get_theme(None)  # type: ignore

            return await ctx.send(f"Your current theme: `{theme}`", ephemeral=True)

        theme, theme_id = theme  # type: ignore

        query = """INSERT INTO user_config (id, theme)
                   VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET
                        theme=EXCLUDED.theme;
                """

        await self.bot.db.execute(query, ctx.author.id, theme_id)

        self.fetch_config.invalidate(self, ctx.author.id)

        await ctx.send(f"Set theme to `{theme}`", ephemeral=True)

    @theme.autocomplete("theme")
    async def theme_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> List[app_commands.Choice[str]]:
        themes = (str(theme) for theme in theme_module.THEMES)
        return [
            app_commands.Choice(name=theme, value=theme)
            for theme in themes if current.lower() in theme.lower()
        ]


async def setup(bot: Logger) -> None:
    await bot.add_cog(Settings(bot))
