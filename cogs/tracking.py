import discord
from discord.ext import commands, tasks

import asyncio
import logging
import asyncpg

log = logging.getLogger("logger.tracking")


class Tracking(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._avatar_batch = []
        self._name_batch = []
        self._nick_batch = []
        self._batch_lock = asyncio.Lock(loop=bot.loop)

        self.bulk_insert_loop.add_exception_type(asyncpg.PostgresConnectionError)
        self.bulk_insert_loop.start()

    def cog_unload(self):
        self.bulk_insert_loop.stop()

    async def bulk_insert(self):
        query = """INSERT INTO avatars (user_id, filename, hash)
                   SELECT x.user_id, x.filename, x.hash
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, filename TEXT, hash TEXT)
                """

        if self._avatar_batch:
            await self.bot.db.execute(query, self._avatar_batch)
            total = len(self._avatar_batch)
            if total > 1:
                log.info("Registered %s avatars to the database.", total)
            self._avatar_batch.clear()

        query = """INSERT INTO names (user_id, name)
                   SELECT x.user_id, x.name
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, name TEXT)
                """

        if self._name_batch:
            await self.bot.db.execute(query, self._name_batch)
            total = len(self._name_batch)
            if total > 1:
                log.info("Registered %s names to the database.", total)
            self._name_batch.clear()

        query = """INSERT INTO nicks (user_id, guild_id, nick)
                   SELECT x.user_id, x.guild_id, x.nick
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, guild_id BIGINT, nick TEXT)
                """

        if self._nick_batch:
            await self.bot.db.execute(query, self._nick_batch)
            total = len(self._nick_batch)
            if total > 1:
                log.info("Registered %s nicks to the database.", total)

            self._nick_batch.clear()

    @tasks.loop(seconds=10.0)
    async def bulk_insert_loop(self):
        async with self._batch_lock:
            await self.bulk_insert()

    @commands.Cog.listener()
    async def on_user_update(self, before, after):

        if before.name != after.name:
            self._name_batch.append({"user_id": after.id, "name": after.name})

        if before.avatar != after.avatar:
            filename = f"{after.id}-{after.avatar}.png"
            await after.avatar_url_as(format="png").save(f"images/{filename}")

            self._avatar_batch.append(
                {"user_id": after.id, "filename": filename, "hash": after.avatar}
            )

    @commands.Cog.listener()
    async def on_member_update(self, before, after):

        if before.nick != after.nick:
            self._nick_batch.append(
                    {
                        "user_id": after.id,
                        "guild_id": after.guild.id,
                        "nick": after.nick,
                    }
                )

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        log.info("Joined a new guild")

        log.info("Loading database")
        nicks = await self.bot.db.fetch("SELECT * FROM nicks;")

        nick_batch = []

        log.info("Updating nicknames")
        for member in guild.members:
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

        if nick_batch:
            await self.bot.db.execute(query, nick_batch)
            total = len(nick_batch)
            if total > 1:
                log.info("Registered %s nicks to the database.", total)

        log.info("Updating avatars and usernames")
        await self.bot.update_users()

    @commands.Cog.listener()
    async def on_member_join(self, user):
        log.info("Member joined a guild")

        log.info("Loading database")

        query = """SELECT *
                   FROM avatars
                   WHERE avatars.user_id=$1;
                """
        user_avatars = await self.bot.db.fetch(query, user.id)

        query = """SELECT *
                   FROM names
                   WHERE names.user_id=$1;
                """
        user_names = await self.bot.db.fetch(query, user.id)

        log.info("Updating database")

        if user.avatar and (
            not user_avatars or user_avatars[-1]["hash"] != user.avatar
        ):
            filename = f"{user.id}-{user.avatar}.png"
            await user.avatar_url_as(format="png").save(f"images/{filename}")

            query = """INSERT INTO avatars (user_id, filename, hash)
                        VALUES ($1, $2, $3);
                    """
            await self.bot.db.execute(query, user.id, filename, user.avatar)

        if not user_names or user_names[-1]["name"] != user.name:
            query = """INSERT INTO names (user_id, name)
                        VALUES ($1, $2);
                    """
            await self.bot.db.execute(query, user.id, user.name)


def setup(bot):
    bot.add_cog(Tracking(bot))
