import discord
from discord.ext import commands

import sys
import traceback

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

def setup(bot):
    bot.add_cog(Meta(bot))
