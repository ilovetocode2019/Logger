from typing import Optional

import discord
from discord.ext import menus

from .context import Context


class Confirm(menus.Menu):
    def __init__(self, msg: str) -> None:
        super().__init__(timeout=30.0, delete_message_after=True)
        self.msg = msg
        self.result = None

    async def send_initial_message(self, ctx: Context, channel: discord.abc.Messageable) -> discord.Message:
        return await channel.send(self.msg)

    @menus.button("\N{WHITE HEAVY CHECK MARK}")
    async def do_confirm(self, payload: discord.RawReactionActionEvent) -> None:
        self.result = True
        self.stop()

    @menus.button("\N{CROSS MARK}")
    async def do_deny(self, payload: discord.RawReactionActionEvent) -> None:
        self.result = False
        self.stop()

    async def prompt(self, ctx: Context) -> Optional[bool]:
        await self.start(ctx, wait=True)
        return self.result
