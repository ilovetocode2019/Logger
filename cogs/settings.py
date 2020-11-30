from discord.ext import commands
import discord

from .utils import cache
from .utils import theme as theme_module


class UserConfig:
    @classmethod
    def from_record(cls, record):
        self = cls()

        self.id = record["id"]
        self.theme = theme_module.get_theme(record["theme"])

        return self


class ThemeConverter(commands.Converter):
    async def convert(self, ctx, arg):
        arg = arg.lower()

        for k, v in theme_module.THEME_MAPPING.items():
            if str(v) == arg:
                return v, k

        raise commands.BadArgument("Invalid theme provided")


class Settings(commands.Cog):
    """Commands to configure the bot"""

    def __init__(self, bot):
        self.bot = bot

    @cache.cache()
    async def fetch_config(self, user_id):
        query = """SELECT *
                   FROM user_config
                   WHERE id=$1;
                """

        record = await self.bot.db.fetchrow(query, user_id)

        if not record:
            return None

        return UserConfig.from_record(record)

    @commands.group(aliases=["config"], invoke_without_command=True)
    async def settings(self, ctx):
        """Configure your settings"""
        await ctx.send_help(ctx.command)

    @settings.command(name="theme")
    async def settings_theme(self, ctx, *, theme: ThemeConverter = None):
        """View or change your theme

        Available themes:
         - light (default): The basic discord light theme
         - dark: The basic discord dark theme
        """
        if not theme:
            config = await self.fetch_config(ctx.author.id)
            theme = config.theme if config else theme_module.get_theme(None)

            return await ctx.send(f"Your current theme: `{theme}`")

        theme, theme_id = theme

        query = """INSERT INTO user_config (id, theme)
                   VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET
                        theme=EXCLUDED.theme;
                """

        await self.bot.db.execute(query, ctx.author.id, theme_id)

        self.fetch_config.invalidate(ctx.author.id)

        await ctx.send(f"Set theme to `{theme}`")


def setup(bot):
    bot.add_cog(Settings(bot))
