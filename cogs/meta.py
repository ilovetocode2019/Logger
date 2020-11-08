import discord
from discord.ext import commands

import sys
import traceback
import datetime
import humanize

class Meta(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        print("Ignoring exception in command {}:".format(ctx.command), file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )

        if isinstance(error, discord.ext.commands.errors.BotMissingPermissions):
            perms_text = "\n".join(
                [
                    f"- {perm.replace('_', ' ').capitalize()}"
                    for perm in error.missing_perms
                ]
            )
            return await ctx.send(f":x: Missing Permissions:\n {perms_text}")
        elif isinstance(error, discord.ext.commands.errors.BadArgument):
            return await ctx.send(f":x: {error}")
        elif isinstance(error, discord.ext.commands.errors.MissingRequiredArgument):
            return await ctx.send(f":x: {error}")
        elif isinstance(error, discord.ext.commands.errors.CommandNotFound):
            return
        elif isinstance(error, discord.ext.commands.errors.CheckFailure):
            return

        await ctx.send(f"```py\n{error}\n```")

    @commands.command(name="invite", description="Get an invite link")
    async def invite(self, ctx):
        invite = discord.utils.oauth_url(self.bot.user.id)
        await ctx.send(f"<{invite}>")

    @commands.command(name="ping", description="Check my latency")
    async def ping(self, ctx):
        await ctx.send(f"My latency is {int(self.bot.latency*1000)}ms")

    @commands.command(name="uptime", description="Check my uptime")
    async def uptime(self, ctx):
        delta = datetime.datetime.utcnow()-self.bot.startup_time
        await ctx.send(f"I started up {humanize.naturaldelta(delta)} ago")

def setup(bot):
    bot.add_cog(Meta(bot))
