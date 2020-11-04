import discord
from discord.ext import commands

import asyncpg
import aiohttp
import asyncio
import os
import logging

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

        log.info("Connecting to database")
        self.db = await asyncpg.connect(config.database_uri)

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
        names = await self.db.fetch("SELECT * FROM names;")
        avatars = await self.db.fetch("SELECT * FROM avatars;")

        for user in bot.users:
            user_avatars = [avatar for avatar in avatars if avatar["user_id"] == user.id]
            if user.avatar and (not user_avatars or user_avatars[-1]["hash"] != user.avatar):
                filename = f"{user.id}-{user.avatar}.png"
                await user.avatar_url_as(format="png").save(f"images/{filename}")

                query = """INSERT INTO avatars (user_id, filename, hash)
                           VALUES ($1, $2, $3);
                        """
                await self.db.execute(query, user.id, filename, user.avatar)

            user_names = [name for name in names if name["user_id"] == user.id]
            if not user_names or user_names[-1]["name"] != user.name:
                query = """INSERT INTO names (user_id, name)
                           VALUES ($1, $2);
                        """
                await self.db.execute(query, user.id, user.name)

    async def on_ready(self):
        log.info("Loading database")
        nicks = await self.db.fetch("SELECT * FROM nicks;")

        log.info("Preparing database")

        for member in self.get_all_members():
            member_nicks = [nick for nick in nicks if nick["user_id"] == member.id and nick["guild_id"] == member.guild.id]
            if member.nick and (not member_nicks or member_nicks[-1]["nick"] != member.nick):
                query = """INSERT INTO nicks (user_id, guild_id, nick)
                           VALUES ($1, $2, $3);
                        """
                await self.db.execute(query, member.id, member.guild.id, member.nick)

        await self.update_users()

        log.info(f"Logged in as {self.user.name} - {self.user.id}")

    def run(self):
        super().run(config.token)

bot = Logger()
bot.run()
