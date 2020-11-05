import discord
from discord.ext import commands, tasks

import asyncio
import logging
import asyncpg
import humanize
import datetime
import io
import functools
import os
from PIL import Image

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
            if after.avatar:
                filename = f"{after.id}-{after.avatar}.png"
                await after.avatar_url_as(format="png").save(f"images/{filename}")

                self._avatar_batch.append(
                    {"user_id": after.id, "filename": filename, "hash": after.avatar}
                )
            else:
                avatar = int(after.discriminator) % 5
                filename = f"{avatar}.png"
                async with self.bot.session.get(
                    f"https://cdn.discordapp.com/embed/avatars/{avatar}.png"
                ) as resp:
                    with open(f"images/{filename}", "wb") as f:
                        f.write(await resp.read())

                self._avatar_batch.append(
                    {"user_id": after.id, "filename": filename, "hash": None}
                )

    @commands.Cog.listener()
    async def on_member_update(self, before, after):

        if after.nick and before.nick != after.nick:
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

        if not user_avatars or user_avatars[-1]["hash"] != user.avatar:
            if user.avatar:
                try:
                    filename = f"{user.id}-{user.avatar}.png"
                    await user.avatar_url_as(format="png").save(f"images/{filename}")

                    query = """INSERT INTO avatars (user_id, filename, hash)
                                VALUES ($1, $2, $3);
                            """
                    await self.bot.db.execute(query, user.id, filename, user.avatar)
                except discord.NotFound:
                    log.warning(
                        f"Failed to fetch avatar for {user} ({user.id}). Ignoring."
                    )
            else:
                avatar = int(user.discriminator) % 5
                filename = f"{avatar}.png"
                async with self.bot.session.get(
                    f"https://cdn.discordapp.com/embed/avatars/{avatar}.png"
                ) as resp:
                    with open(f"images/{filename}", "wb") as f:
                        f.write(await resp.read())

                self._avatar_batch.append(
                    {"user_id": user.id, "filename": filename, "hash": None}
                )

        if not user_names or user_names[-1]["name"] != user.name:
            query = """INSERT INTO names (user_id, name)
                        VALUES ($1, $2);
                    """
            await self.bot.db.execute(query, user.id, user.name)

    @commands.command(name="names", description="View past usernames for a user")
    async def names(self, ctx, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        query = """SELECT *
                   FROM names
                   WHERE names.user_id=$1
                   ORDER BY names.recorded_at DESC;
                """
        names = await self.bot.db.fetch(query, user.id)

        content = ""
        for name in names:
            recorded_at = name["recorded_at"]
            timedelta = datetime.datetime.utcnow() - recorded_at
            content += f"\n{name['name']} - {humanize.naturaldate(recorded_at)} ({humanize.naturaldelta(timedelta)} ago)"

        await ctx.send(content)

        await ctx.send("\n".join(names))

    @commands.command(name="nicks", description="View past nicknames for a user")
    async def nicks(self, ctx, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        query = """SELECT *
                   FROM nicks
                   WHERE nicks.user_id=$1 AND nicks.guild_id=$2
                   ORDER BY nicks.recorded_at DESC;
                """
        nicks = await self.bot.db.fetch(query, user.id, ctx.guild.id)

        if not nicks:
            return await ctx.send(":x: You have no nicknames for this server")

        content = ""
        for nick in nicks:
            recorded_at = nick["recorded_at"]
            timedelta = datetime.datetime.utcnow() - recorded_at
            content += f"\n{nick['nick']} - {humanize.naturaldate(recorded_at)} ({humanize.naturaldelta(timedelta)} ago)"

        await ctx.send(content)

    @commands.command(name="avatars", descripion="Avatars")
    async def avatars(self, ctx, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        query = """SELECT *
                   FROM avatars
                   WHERE avatars.user_id=$1
                   ORDER BY avatars.recorded_at DESC;
                """
        avatars = await self.bot.db.fetch(query, user.id)

        partial = functools.partial(self.draw_image, avatars)
        file = await self.bot.loop.run_in_executor(None, partial)
        file.seek(0)
        await ctx.send(file=discord.File(fp=file, filename="image.png"))

    def draw_image(self, avatars):
        file = io.BytesIO()

        if len(avatars) != 1:
            counter = 2
            while True:
                boxes = counter**2
                if boxes >= len(avatars):
                    columns = counter
                    break
                counter += 1

            if columns > 3:
                size = 4096
            else:
                size = 2048

            side_legnth = int(size/columns)

            rows = 1
            column = 0
            for avatar in avatars:
                if column == columns:
                    rows += 1
                    column = 0

                column += 1

            image = Image.new("RGBA", (size, rows*side_legnth), (255, 0, 0, 0))

            column = 0
            row = 0
            for avatar in avatars:
                avatar = Image.open(f"images/{avatar['filename']}")
                avatar = avatar.resize((side_legnth, side_legnth))

                image.paste(avatar, (column*side_legnth, row*side_legnth))

                column += 1
                if column == columns:
                    row += 1
                    column = 0

            image.save(file, "PNG")

        else:
            image = Image.open(f"images/{avatars[0]['filename']}")

            image.save(file, "PNG")

        return file

    @commands.command(name="avatar", description="View a specific avatar in history")
    async def avatar(self, ctx, avatar: int, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        query = """SELECT *
                   FROM avatars
                   WHERE avatars.user_id=$1
                   ORDER BY avatars.recorded_at DESC;
                """
        avatars = await self.bot.db.fetch(query, user.id)

        try:
            avatar = avatars[avatar-1]
        except IndexError:
            return await ctx.send(":x: Invalid avatar index")

        em = discord.Embed(timestamp=avatar["recorded_at"])
        em.set_author(name=user.display_name, icon_url=user.avatar_url)
        em.set_image(url="attachment://image.png")
        em.set_footer(text="Recorded")

        await ctx.send(content=f"Hash: {avatar['hash']}", embed=em, file=discord.File(f"images/{avatar['filename']}", filename="image.png"))

def setup(bot):
    bot.add_cog(Tracking(bot))
