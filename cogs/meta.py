from __future__ import annotations

import datetime
import sys
import traceback
from typing import TYPE_CHECKING

import discord
import humanize
from discord.ext import commands

from .utils.context import Context

if TYPE_CHECKING:
    from bot import Logger

class HelpCommand(commands.MinimalHelpCommand):
    def get_command_signature(self, command):
        return "{0.clean_prefix}{1.qualified_name} {1.signature}".format(self, command)

class Meta(commands.Cog):
    def __init__(self, bot: Logger):
        self.bot = bot
        self._original_help_command = bot.help_command
        bot.help_command = HelpCommand()
        bot.help_command.cog = self

    def cog_unload(self):
        self.bot.help_command = self._original_help_command

    @commands.Cog.listener()
    async def on_command_error(self, ctx: Context, error):
        print("Ignoring exception in command {}:".format(ctx.command), file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )

        if isinstance(error, commands.errors.BotMissingPermissions):
            perms_text = "\n".join(
                [
                    f"- {perm.replace('_', ' ').capitalize()}"
                    for perm in error.missing_permissions
                ]
            )
            return await ctx.send(f":x: Missing Permissions:\n {perms_text}", ephemeral=True)
        elif isinstance(error, commands.errors.BadArgument):
            return await ctx.send(f":x: {error}", ephemeral=True)
        elif isinstance(error, commands.errors.MissingRequiredArgument):
            return await ctx.send(f":x: {error}", ephemeral=True)
        elif isinstance(error, commands.errors.CommandNotFound):
            return
        elif isinstance(error, commands.errors.CheckFailure):
            return

        await ctx.send(f"```py\n{error}\n```")

        if isinstance(error, commands.CommandInvokeError):
            em = discord.Embed(title=":warning: Error", description="", color=discord.Color.gold(), timestamp=datetime.datetime.utcnow())

            if TYPE_CHECKING:
                assert isinstance(em.description, str)

            em.description += f"\nCommand: `{ctx.command}`"
            em.description += f"\nLink: [Jump]({ctx.message.jump_url})"
            em.description += f"\n\n```py\n{error}```\n"

            if not isinstance(self.bot.console, discord.abc.Messageable):
                self.bot.log.warning("Bot console is not messageable.")
                return

            await self.bot.console.send(embed=em)

    @commands.command(name="invite", description="Get an invite link")
    async def invite(self, ctx: Context):
        invite = discord.utils.oauth_url(self.bot.user.id)  # type: ignore
        await ctx.send(f"<{invite}>")

    @commands.hybrid_command(name="ping", description="Check my latency")
    async def ping(self, ctx: Context):
        await ctx.send(f"My latency is {int(self.bot.latency*1000)}ms")

    @commands.hybrid_command(name="uptime", description="Check my uptime")
    async def uptime(self, ctx: Context):
        delta = datetime.datetime.utcnow()-self.bot.startup_time
        await ctx.send(f"I started up {humanize.naturaldelta(delta)} ago")

async def setup(bot: Logger) -> None:
    await bot.add_cog(Meta(bot))
