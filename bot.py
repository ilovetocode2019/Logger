import discord
from discord.ext import commands

import asyncpg
import aiohttp
import asyncio
import os
import logging
import json
import asyncio
import datetime

import config

logging.basicConfig(
    level=logging.INFO,
    format="(%(asctime)s) %(levelname)s %(message)s",
    datefmt="%m/%d/%y - %H:%M:%S %Z",
)

log = logging.getLogger("logger")


class Logger(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=config.prefix, intents=discord.Intents.all(), owner_ids=config.owner_ids)
        self.db_ready = asyncio.Event()
        self.startup_time = datetime.datetime.utcnow()

        self.log = log

        self.loop.create_task(self.prepare_bot())

        self.cogs_to_add = ["cogs.admin", "cogs.meta", "cogs.tracking", "cogs.settings"]
        self.load_extension("jishaku")
        for cog in self.cogs_to_add:
            self.load_extension(cog)

    async def wait_until_db_ready(self):
        if not self.db_ready.is_set():
            await self.db_ready.wait()

    async def prepare_bot(self):
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

        self.db = await asyncpg.create_pool(config.database_uri, init=init)

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
                   guild_id BIGINT,
                   status TEXT,
                   recorded_at TIMESTAMP DEFAULT (now() at time zone 'utc')
                   );

                   CREATE TABLE IF NOT EXISTS user_config (
                   id BIGINT PRIMARY KEY,
                   theme INTEGER DEFAULT 0
                   );
                """
        await self.db.execute(query)

    async def update_users(self):
        log.info("Querying user avatar and name changes")

        names = await self.db.fetch("SELECT * FROM names;")
        avatars = await self.db.fetch("SELECT * FROM avatars;")

        avatar_batch = []
        name_batch = []

        for user in bot.users:
            user_avatars = [
                avatar for avatar in avatars if avatar["user_id"] == user.id
            ]
            if not user_avatars or user_avatars[-1]["hash"] != user.avatar:

                if user.avatar:
                    try:
                        filename = f"{user.id}-{user.avatar}.png"
                        await user.avatar_url_as(format="png").save(f"images/{filename}")

                        avatar_batch.append(
                            {"user_id": user.id, "filename": filename, "hash": user.avatar}
                        )
                    except discord.NotFound:
                        log.warning(f"Failed to fetch avatar for {user} ({user.id}). Ignoring.")

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
            log.info("Registered %s avatar(s) to the database.", total)
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
            log.info("Registered %s name(s) to the database.", total)
        else:
            log.info("No work needed for names")

        self.db_ready.set()

    async def on_ready(self):
        log.info(f"Logged in as {self.user.name} - {self.user.id}")

        self.console = bot.get_channel(config.console)

        log.info("Loading database")
        nicks = await self.db.fetch("SELECT * FROM nicks;")
        presences = await self.db.fetch("SELECT * FROM presences;")

        log.info("Preparing database")

        log.info("Querying member, nick, and presence changes")

        nick_batch = []
        presence_batch = []

        for member in self.get_all_members():
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
            log.info("Registered %s nick(s) to the database.", total)
        else:
            log.info("No work needed for nicks")

        query = """INSERT INTO presences (user_id, guild_id, status)
                   SELECT x.user_id, x.guild_id, x.status
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, guild_id BIGINT, status TEXT)
                """

        if presence_batch:
            await self.db.execute(query, presence_batch)
            total = len(presence_batch)
            log.info("Registered %s presence(s) to database.", total)
        else:
            log.info("No work needed to presences")

        await self.update_users()

        log.info("Database is now up-to-date")

    def run(self):
        super().run(config.token)

    async def logout(self):
        await self.db.close()
        await self.session.close()
        await super().logout()

bot = Logger()
bot.run()
