import discord
from discord.ext import commands

import asyncpg
import aiohttp
import asyncio
import os
import logging
import json

import config

logging.basicConfig(
    level=logging.INFO,
    format="(%(asctime)s) %(levelname)s %(message)s",
    datefmt="%m/%d/%y - %H:%M:%S %Z",
)

log = logging.getLogger("logger")


class Logger(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=config.prefix, intents=discord.Intents.all())

        self.loop.create_task(self.prepare_bot())

        self.cogs_to_add = ["cogs.meta", "cogs.tracking"]
        self.load_extension("jishaku")
        for cog in self.cogs_to_add:
            self.load_extension(cog)

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
            if user.avatar and (
                not user_avatars or user_avatars[-1]["hash"] != user.avatar
            ):
                filename = f"{user.id}-{user.avatar}.png"
                await user.avatar_url_as(format="png").save(f"images/{filename}")

                avatar_batch.append(
                    {"user_id": user.id, "filename": filename, "hash": user.avatar}
                )

            user_names = [name for name in names if name["user_id"] == user.id]
            if not user_names or user_names[-1]["name"] != user.name:
                name_batch.append({"user_id": user.id, "name": user.name})

        query = """INSERT INTO avatars (user_id, filename, hash)
                   SELECT x.user_id, x.filename, x.hash
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, filename TEXT, hash TEXT)
                """

        changed = False

        if avatar_batch:
            await self.db.execute(query, avatar_batch)
            total = len(avatar_batch)
            if total > 1:
                log.info("Registered %s avatars to the database.", total)
                changed = True

        if not changed:
            log.info("No work needed for avatars")

        changed = False

        query = """INSERT INTO names (user_id, name)
                   SELECT x.user_id, x.name
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, name TEXT)
                """

        if name_batch:
            await self.db.execute(query, name_batch)
            total = len(name_batch)
            if total > 1:
                log.info("Registered %s names to the database.", total)
                changed = True

        if not changed:
            log.info("No work needed for names")

    async def on_ready(self):
        log.info(f"Logged in as {self.user.name} - {self.user.id}")

        log.info("Loading database")
        nicks = await self.db.fetch("SELECT * FROM nicks;")

        log.info("Preparing database")

        log.info("Querying member nick changes")
        nick_batch = []

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

        query = """INSERT INTO nicks (user_id, guild_id, nick)
                   SELECT x.user_id, x.guild_id, x.nick
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, guild_id BIGINT, nick TEXT)
                """

        changed = False

        if nick_batch:
            await self.db.execute(query, nick_batch)
            total = len(nick_batch)
            if total > 1:
                log.info("Registered %s nicks to the database.", total)
                changed = True

        if not changed:
            log.info("No work needed for nicks")

        await self.update_users()

        log.info("Database is now up-to-date")

    def run(self):
        super().run(config.token)


bot = Logger()
bot.run()
