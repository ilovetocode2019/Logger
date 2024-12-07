import asyncio
import datetime
import json
import logging
import os
from typing import TYPE_CHECKING, List, Union

import aiohttp
import asyncpg
import discord
from discord.ext import commands

import config
from cogs.utils import formats
from cogs.utils.context import Context

logging.basicConfig(
    level=logging.INFO,
    format="(%(asctime)s) %(levelname)s %(message)s",
    datefmt="%m/%d/%y - %H:%M:%S %Z",
)

log = logging.getLogger("logger")

extensions = ["cogs.admin", "cogs.meta", "cogs.tracking", "cogs.settings"]


class Logger(commands.Bot):

    db: asyncpg.Pool
    log: logging.Logger
    startup_time: datetime.datetime
    session: aiohttp.ClientSession

    def __init__(self):
        super().__init__(command_prefix=config.prefix, intents=discord.Intents.all())

        self.db_lock = asyncio.Lock()
        self.startup_time = None
        self.log = log

    async def setup_hook(self):
        if not os.path.isdir("images"):
            os.mkdir("images")

        self.session = aiohttp.ClientSession()

        async def init(conn):
            await conn.set_type_codec(
                "jsonb",
                schema="pg_catalog",
                encoder=json.dumps,
                decoder=json.loads,
                format="text",
            )

        log.info("Connecting to database.")

        try:
            db = await asyncpg.create_pool(config.database_uri, init=init)
        except Exception:
            log.exception("Failed to connect to database")
            raise
        else:
            if TYPE_CHECKING:
                assert db
            self.db = db

        query = """CREATE TABLE IF NOT EXISTS avatars (
                   id SERIAL PRIMARY KEY,
                   user_id BIGINT,
                   filename TEXT,
                   hash TEXT,
                   recorded_at TIMESTAMP DEFAULT (now() at time zone 'utc')
                   );

                   CREATE TABLE IF NOT EXISTS nicks (
                   id SERIAL PRIMARY KEY,
                   user_id BIGINT,
                   guild_id BIGINT,
                   nick TEXT,
                   recorded_at TIMESTAMP DEFAULT (now() at time zone 'utc')
                   );

                   CREATE TABLE IF NOT EXISTS names (
                   id SERIAL PRIMARY KEY,
                   user_id BIGINT,
                   name TEXT,
                   recorded_at TIMESTAMP DEFAULT (now() at time zone 'utc')
                   );

                   CREATE TABLE IF NOT EXISTS presences (
                   id SERIAL PRIMARY KEY,
                   user_id BIGINT,
                   status TEXT,
                   recorded_at TIMESTAMP DEFAULT (now() at time zone 'utc')
                   );

                   CREATE TABLE IF NOT EXISTS user_config (
                   id BIGINT PRIMARY KEY,
                   theme INTEGER DEFAULT 0
                   );
                """
        await self.db.execute(query)

        await self.load_extension("jishaku")

        for extension in extensions:
            await self.load_extension(extension)

    async def get_user_updates(self, users: List[Union[discord.User, discord.Member]]):
        query = """SELECT DISTINCT ON (avatars.user_id) *
                   FROM avatars
                   ORDER BY avatars.user_id, avatars.recorded_at DESC
                """

        avatars = {avatar["user_id"]: avatar for avatar in await self.db.fetch(query)}

        query = """SELECT DISTINCT ON (names.user_id) *
                   FROM names
                   ORDER BY names.user_id, names.recorded_at DESC
                """

        names = {name["user_id"]: name for name in await self.db.fetch(query)}


        avatar_batch = []
        name_batch = []

        for user in users:
            if user.id not in avatars or avatars[user.id]["hash"] != user.display_avatar.key:
                try:
                    filename = f"{f'{user.id}-' if user.avatar else ''}{user.display_avatar.key}.png"
                    await user.display_avatar.with_format("png").save(f"images/{filename}")

                    avatar_batch.append({
                        "user_id": user.id,
                        "filename": filename,
                        "hash": user.display_avatar.key
                    })
                except discord.NotFound:
                    log.warning(
                        "Failed to fetch avatar %s for %s (%s). Ignoring.",
                        user.display_avatar.url,
                        user.name,
                        user.id
                    )

            if user.id not in names or names[user.id]["name"] != user.name:
                name_batch.append({"user_id": user.id, "name": user.name})

        return avatar_batch, name_batch

    async def get_member_updates(self, members: List[discord.Member]):
        query = """SELECT DISTINCT ON (nicks.guild_id, nicks.user_id) *
                   FROM nicks
                   ORDER BY nicks.guild_id, nicks.user_id, nicks.recorded_at DESC
                """

        nicks = {(nick["user_id"], nick["guild_id"]): nick for nick in await self.db.fetch(query)}

        query = """SELECT DISTINCT ON (presences.user_id) *
                   FROM presences
                   ORDER BY presences.user_id, presences.recorded_at DESC;
                """

        presences = {presence["user_id"]: presence for presence in await self.db.fetch(query)}

        nick_batch = []
        presence_batch = []
        _processed_presences = []

        for member in members:
            if member.nick and ((member.id, member.guild.id) not in nicks or nicks[(member.id, member.guild.id)]["nick"] != member.nick):
                nick_batch.append({
                    "user_id": member.id,
                    "guild_id": member.guild.id,
                    "nick": member.nick,
                })

            if (member.id not in presences or presences[member.id]["status"] != str(member.status)) and member.id not in _processed_presences:
                _processed_presences.append(member.id)
                presence_batch.append({"user_id": member.id, "status": str(member.status)})

        return nick_batch, presence_batch

    async def on_ready(self):
        log.info("Logged in as %s - %s.", self.user.name, self.user.id)

        self.console = bot.get_channel(config.console)

        if not self.startup_time:
            self.startup_time = discord.utils.utcnow()

        async with self.db_lock:
            # checking for missed events or initial startup
            users = [discord.User._copy(user) for user in bot.users]
            members =[discord.Member._copy(member) for member in self.get_all_members()]

            log.info("Looking for user related changes...")
            avatar_batch, name_batch = await self.get_user_updates(users)

            log.info("Looking for member related changes...")
            nick_batch, presence_batch = await self.get_member_updates(members)

            log.info("Applying changes to the database...")

            query = """INSERT INTO avatars (user_id, filename, hash)
                       SELECT x.user_id, x.filename, x.hash
                       FROM jsonb_to_recordset($1::jsonb) AS
                       x(user_id BIGINT, filename TEXT, hash TEXT);
                    """
            await self.db.execute(query, avatar_batch)

            query = """INSERT INTO names (user_id, name)
                       SELECT x.user_id, x.name
                       FROM jsonb_to_recordset($1::jsonb) AS
                       x(user_id BIGINT, name TEXT)
                    """
            await self.db.execute(query, name_batch)

            query = """INSERT INTO nicks (user_id, guild_id, nick)
                       SELECT x.user_id, x.guild_id, x.nick
                       FROM jsonb_to_recordset($1::jsonb) AS
                       x(user_id BIGINT, guild_id BIGINT, nick TEXT);
                    """
            await self.db.execute(query, nick_batch)

            query = """INSERT INTO presences (user_id, status)
                       SELECT x.user_id, x.status
                       FROM jsonb_to_recordset($1::jsonb) AS
                       x(user_id BIGINT, guild_id BIGINT, status TEXT);
                    """
            await self.db.execute(query, presence_batch)

            log.info(
                "Registered %s, %s, %s, and %s to the database on startup.",
                format(formats.plural(len(avatar_batch)), "avatar"),
                format(formats.plural(len(name_batch)), "name"),
                format(formats.plural(len(nick_batch)), "nick"),
                format(formats.plural(len(presence_batch)), "presence")
            )

    async def get_context(
        self,
        origin: Union[discord.Message, discord.Interaction],
        cls=None,
    ) -> Context:
        return await super().get_context(origin, cls=cls or Context)

    def run(self):
        super().run(config.token)

    async def close(self):
        await self.db.close()
        await self.session.close()
        await super().close()

bot = Logger()
bot.run()
