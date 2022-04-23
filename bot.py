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


class Logger(commands.Bot):

    db: asyncpg.Pool
    log: logging.Logger
    startup_time: datetime.datetime
    session: aiohttp.ClientSession

    def __init__(self):
        super().__init__(command_prefix=config.prefix, intents=discord.Intents.all())
        self.startup_time = datetime.datetime.utcnow()

        self.log = log

    async def setup_hook(self):
        self.db_ready = asyncio.Event()

        log.info("Preparing image directory")
        if not os.path.isdir("images"):
            os.mkdir("images")

        log.info("Creating aiohttp session")
        self.session = aiohttp.ClientSession()

        async def init(conn):
            await conn.set_type_codec(
                "jsonb",
                schema="pg_catalog",
                encoder=json.dumps,
                decoder=json.loads,
                format="text",
            )

        log.info("Connecting to database")

        try:
            db = await asyncpg.create_pool(config.database_uri, init=init)
        except Exception:
            log.exception("Failed to connect to database")
            raise
        else:
            if TYPE_CHECKING:
                assert db
            self.db = db

        log.info("Initiating database")

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

        self.cogs_to_add = ["cogs.admin", "cogs.meta", "cogs.tracking", "cogs.settings"]
        await self.load_extension("jishaku")
        for cog in self.cogs_to_add:
            await self.load_extension(cog)

    async def wait_until_db_ready(self):
        if not self.db_ready.is_set():
            await self.db_ready.wait()

    async def update_users(self, users: Union[List[discord.User], List[discord.Member]]):
        names = await self.db.fetch("SELECT * FROM names;")
        avatars = await self.db.fetch("SELECT * FROM avatars;")

        avatar_batch = []
        name_batch = []

        for user in users:
            user_avatars = [
                avatar for avatar in avatars if avatar["user_id"] == user.id
            ]
            avatar_key = user.avatar.key if user.avatar else None
            if not user_avatars or user_avatars[-1]["hash"] != avatar_key:

                if user.avatar:
                    try:
                        filename = f"{user.id}-{user.avatar.key}.png"
                        await user.avatar.with_format("png").save(f"images/{filename}")

                        avatar_batch.append(
                            {"user_id": user.id, "filename": filename, "hash": user.avatar.key}
                        )
                    except discord.NotFound:
                        log.warning(f"Failed to fetch avatar for {user} ({user.id}). Ignoring")

                else:
                    avatar = int(user.discriminator)%5
                    filename = f"{avatar}.png"
                    async with self.session.get(f"https://cdn.discordapp.com/embed/avatars/{avatar}.png") as resp:
                        with open(f"images/{filename}", "wb") as f:
                            f.write(await resp.read())

                    avatar_batch.append(
                        {"user_id": user.id, "filename": filename, "hash": None}
                    )

            user_names = [name for name in names if name["user_id"] == user.id]
            if not user_names or user_names[-1]["name"] != user.name:
                name_batch.append({"user_id": user.id, "name": user.name})

        query = """INSERT INTO avatars (user_id, filename, hash)
                   SELECT x.user_id, x.filename, x.hash
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, filename TEXT, hash TEXT)
                """
        if avatar_batch:
            await self.db.execute(query, avatar_batch)
            total = len(avatar_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "avatar"))
        else:
            log.info("No work needed for avatars")

        query = """INSERT INTO names (user_id, name)
                   SELECT x.user_id, x.name
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, name TEXT)
                """
        if name_batch:
            await self.db.execute(query, name_batch)
            total = len(avatar_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "name"))
        else:
            log.info("No work needed for names")

    async def on_ready(self):
        log.info(f"Logged in as {self.user.name} - {self.user.id}")  #type: ignore

        self.console = bot.get_channel(config.console)

        log.info("Loading database")
        nicks = await self.db.fetch("SELECT * FROM nicks;")
        presences = await self.db.fetch("SELECT * FROM presences;")

        log.info("Loading all members and users")
        users = [discord.User._copy(user) for user in bot.users]
        members = [discord.Member._copy(member) for member in self.get_all_members()]

        log.info("Preparing database")

        log.info("Querying nick, and presence changes")

        nick_batch = []
        presence_batch = []

        for member in members:
            member_nicks = [
                nick
                for nick in nicks
                if nick["user_id"] == member.id and nick["guild_id"] == member.guild.id
            ]
            if member.nick and (
                not member_nicks or member_nicks[-1]["nick"] != member.nick
            ):
                nick_batch.append(
                    {
                        "user_id": member.id,
                        "guild_id": member.guild.id,
                        "nick": member.nick,
                    }
                )

            member_presences = [
                presence
                for presence in presences
                if presence["user_id"] == member.id
            ]
            if (not member_presences or member_presences[-1]["status"] != str(member.status)) and member.id not in [presence["user_id"] for presence in presence_batch]:
                presence_batch.append(
                    {
                        "user_id": member.id,
                        "status": str(member.status)
                    }
                )

        query = """INSERT INTO nicks (user_id, guild_id, nick)
                   SELECT x.user_id, x.guild_id, x.nick
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, guild_id BIGINT, nick TEXT)
                """
        if nick_batch:
            await self.db.execute(query, nick_batch)
            total = len(nick_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "nick"))
        else:
            log.info("No work needed for nicks")

        query = """INSERT INTO presences (user_id, status)
                   SELECT x.user_id, x.status
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, guild_id BIGINT, status TEXT)
                """
        if presence_batch:
            await self.db.execute(query, presence_batch)
            total = len(presence_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "presence"))
        else:
            log.info("No work needed to presences")

        log.info("Querying avatar and name changes")
        await self.update_users(users)
        log.info("Database is now up-to-date")
        self.db_ready.set()

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
