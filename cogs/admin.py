import discord
from discord.ext import commands, menus, tasks

import traceback
import importlib
import asyncio
import subprocess
import sys
import re
import os
import time
import traceback
import io
import pkg_resources
from jishaku.codeblocks import codeblock_converter

class Confirm(menus.Menu):
    def __init__(self, msg):
        super().__init__(timeout=30.0, delete_message_after=True)
        self.msg = msg
        self.result = None

    async def send_initial_message(self, ctx, channel):
        return await channel.send(self.msg)

    @menus.button("\N{WHITE HEAVY CHECK MARK}")
    async def do_confirm(self, payload):
        self.result = True
        self.stop()

    @menus.button("\N{CROSS MARK}")
    async def do_deny(self, payload):
        self.result = False
        self.stop()

    async def prompt(self, ctx):
        await self.start(ctx, wait=True)
        return self.result


class Admin(commands.Cog):
    """Admin commands and features for owners of the bot"""

    def __init__(self, bot):
        self.bot = bot
        self.update_loop.start()

    def cog_unload(self):
        self.update_loop.cancel()

    async def cog_check(self, ctx):
        return await commands.is_owner().predicate(ctx)

    @commands.group(
        name="reload",
        aliases=["load"],
        invoke_without_command=True,
    )
    async def _reload(self, ctx, *, cog="all"):
        """Reload an extension"""

        if cog == "all":
            msg = ""

            for ext in self.bot.cogs_to_add:
                try:
                    self.bot.reload_extension(ext)
                    msg += (
                        f"**\N{CLOCKWISE RIGHTWARDS AND LEFTWARDS OPEN CIRCLE ARROWS} Reloaded** `{ext}`\n\n"
                    )

                except Exception as e:
                    traceback_data = "".join(
                        traceback.format_exception(type(e), e, e.__traceback__, 1)
                    )
                    msg += (
                        f"**\N{CROSS MARK} Extension `{ext}` not loaded.**\n"
                        f"```py\n{traceback_data}```\n\n"
                    )
                    traceback.print_exception(type(e), e, e.__traceback__)
            return await ctx.send(msg)

        try:
            self.bot.reload_extension(cog.lower())
            await ctx.send(f"\N{CLOCKWISE RIGHTWARDS AND LEFTWARDS OPEN CIRCLE ARROWS} **Reloaded** `{cog.lower()}`")
        except Exception as e:
            traceback_data = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, 1)
            )
            await ctx.send(
                f"**\N{CROSS MARK} Extension `{cog.lower()}` not loaded.**\n```py\n{traceback_data}```"
            )
            traceback.print_exception(type(e), e, e.__traceback__)

    # https://github.com/Rapptz/RoboDanny/blob/6211293d8fe19ad46a266ded2464752935a3fb94/cogs/admin.py#L89-L97
    async def run_process(self, command):
        try:
            process = await asyncio.create_subprocess_shell(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            result = await process.communicate()
        except NotImplementedError:
            process = subprocess.Popen(
                command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            result = await self.bot.loop.run_in_executor(None, process.communicate)

        return [output.decode() for output in result]

    # https://github.com/Rapptz/RoboDanny/blob/6211293d8fe19ad46a266ded2464752935a3fb94/cogs/admin.py#L146-L214
    _GIT_PULL_REGEX = re.compile(r"\s*(?P<filename>.+?)\s*\|\s*[0-9]+\s*[+-]+")

    def find_modules_from_git(self, output):
        files = self._GIT_PULL_REGEX.findall(output)
        ret = []
        for file in files:
            root, ext = os.path.splitext(file)
            if ext != ".py":
                continue

            if root.startswith("cogs/"):
                # A submodule is a directory inside the main cog directory for
                # my purposes
                ret.append((root.count("/") - 1, root.replace("/", ".")))

        # For reload order, the submodules should be reloaded first
        ret.sort(reverse=True)
        return ret

    def reload_or_load_extension(self, module):
        try:
            self.bot.reload_extension(module)
        except commands.ExtensionNotLoaded:
            self.bot.load_extension(module)

    @_reload.command(name="all")
    async def _reload_all(self, ctx):
        """Reloads all modules, while pulling from git."""

        async with ctx.typing():
            stdout, stderr = await self.run_process("git pull")

        # progress and stuff is redirected to stderr in git pull
        # however, things like "fast forward" and files
        # along with the text "already up-to-date" are in stdout

        if stdout.startswith("Already up-to-date."):
            return await ctx.send(stdout)

        modules = self.find_modules_from_git(stdout)

        if not modules:
            return await ctx.send("No modules need to be updated.")

        mods_text = "\n".join(
            f"{index}. `{module}`" for index, (_, module) in enumerate(modules, start=1)
        )
        prompt_text = (
            f"This will update the following modules, are you sure?\n{mods_text}"
        )

        confirm = await Confirm(prompt_text).prompt(ctx)
        if not confirm:
            return await ctx.send("Aborting.")

        statuses = []
        for is_submodule, module in modules:
            if is_submodule:
                try:
                    actual_module = sys.modules[module]
                except KeyError:
                    statuses.append(("\N{WHITE HEAVY CHECK MARK}", module))
                else:
                    try:
                        importlib.reload(actual_module)
                    except Exception as e:
                        traceback_data = "".join(
                            traceback.format_exception(type(e), e, e.__traceback__, 1)
                        )
                        statuses.append(
                            ("\N{CROSS MARK}", f"{module}\n```py\n{traceback_data}\n```")
                        )
                    else:
                        statuses.append(("\N{WHITE HEAVY CHECK MARK}", module))
            else:
                try:
                    self.reload_or_load_extension(module)
                except commands.ExtensionError as e:
                    traceback_data = "".join(
                        traceback.format_exception(type(e), e, e.__traceback__, 1)
                    )
                    statuses.append(
                        ("\N{CROSS MARK}", f"{module}\n```py\n{traceback_data}\n```")
                    )
                else:
                    statuses.append(("\N{WHITE HEAVY CHECK MARK}", module))

        await ctx.send("\n".join(f"{status} `{module}`" for status, module in statuses))

    @commands.command(name="sql", description="Run some sql")
    async def sql(self, ctx, *, code: codeblock_converter):
        _, query = code

        execute = query.count(";") > 1

        if execute:
            method = self.bot.db.execute
        else:
            method = self.bot.db.fetch

        try:
            start = time.time()
            results = await method(query)
            end = time.time()
        except Exception as e:
            full = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            return await ctx.send(f"```py\n{full}```")

        if execute:
            return await ctx.send(f"Executed in {int((end-start)*1000)}ms: {str(results)}")

        results = "\n".join([str(record) for record in results])

        if not results:
            return await ctx.send("No results to display")

        try:
            await ctx.send(f"Executed in {int((end-start)*1000)}ms\n```{results}```")
        except discord.HTTPException:
            await ctx.send(file=discord.File(io.BytesIO(str(results).encode("utf-8")), filename="result.txt"))

    @commands.command(name="logout", description="Logs out and shuts down bot")
    async def logout(self, ctx):
        self.bot.log.info("Logging out of Discord")
        await ctx.send("Logging out :wave:")
        await self.bot.logout()

    @tasks.loop(hours=10)
    async def update_loop(self):
        with open("requirements.txt") as file:
            lines = file.read()
            installed = lines.split("\n")

        outdated = []
        for package in installed:
            try:
                current_version = pkg_resources.get_distribution(package).version
                async with self.bot.session.get(f"https://pypi.org/pypi/{package}/json") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pypi_version = data["info"]["version"]
                        if current_version != pypi_version:
                            outdated.append((package, current_version, pypi_version))
            except:
                pass

        if outdated:
            em = discord.Embed(title="Outdated Packages", description="", color=discord.Color.blurple())
            for package in outdated:
                em.description += f"\n{package[0]} (Local: {package[1]} | PyPI: {package[2]})"

            await self.bot.console.send(embed=em)

    @update_loop.before_loop
    async def before_update_loop(self):
        await self.bot.wait_until_ready()

def setup(bot):
    bot.add_cog(Admin(bot))
