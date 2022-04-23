from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

from discord.context_managers import Typing
from discord.ext import commands


class NotTyping:

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        pass


class Context(commands.Context[commands.Bot]):

    def maybe_typing(self)-> Union[Typing, NotTyping]:
        if self.interaction is None or self.interaction.is_expired():
            return Typing(self)
        else:
            return NotTyping()
