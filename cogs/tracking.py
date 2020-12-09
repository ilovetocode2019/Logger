import discord
from discord.ext import commands, tasks

import asyncio
import logging
import asyncpg
import humanize
import datetime
import calendar
import io
import functools
import os
from PIL import Image, ImageDraw, ImageFont

from .utils.theme import get_theme

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

    async def get_presence_batch(self):
        presences = await self.bot.db.fetch("SELECT * FROM presences;")

        presence_batch = []
        for member in self.bot.get_all_members():
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

        return presence_batch


    async def bulk_insert(self):
        presence_batch = await self.get_presence_batch()

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

        query = """INSERT INTO presences (user_id, status)
                   SELECT x.user_id, x.status
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, status TEXT)
                """

        if presence_batch:
            await self.bot.db.execute(query, presence_batch)
            total = len(presence_batch)
            if total > 1:
                log.info("Registered %s presences to the database.", total)
            presence_batch.clear()

    @tasks.loop(seconds=20.0)
    async def bulk_insert_loop(self):
        async with self._batch_lock:
            await self.bulk_insert()

    @bulk_insert_loop.before_loop
    async def before_bulk_insert_loop(self):
        await self.bot.wait_until_db_ready()

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
        presences = await self.bot.db.fetch("SELECT * FROM presences;")

        nick_batch = []
        presence_batch = []

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

            member_presences = [
                presence
                for presence in presences
                if presence["user_id"] == member.id
            ]
            if not member_presences or member_presences[-1]["status"] != str(member.status):
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
            await self.bot.db.execute(query, nick_batch)
            total = len(nick_batch)
            if total > 1:
                log.info("Registered %s nicks to the database.", total)

        query = """INSERT INTO presences (user_id, status)
                   SELECT x.user_id, x.status
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, status TEXT)
                """
        if presence_batch:
            await self.bot.db.execute(query, presence_batch)
            total = len(presence_batch)
            if total > 1:
                log.info("Registered %s presences to the database.", total)

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

        query = """SELECT *
                   FROM presences
                   WHERE presences.user_id=$1;
                """
        user_presences = await self.bot.db.fetch(query, user.id)

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

        if not user_presences or user_presences[-1]["status"] != str(user.status):
            query = """INSERT INTO presences (user_id, status)
                       VALUES ($1, $2);
                    """
            await self.bot.db.execute(query, user.id, str(user.status))

    @commands.Cog.listener()
    async def on_member_leave(self, user):
        shared = len([guild for guild in self.bot.guilds if user.id in [member.id for member in guild.members]])
        if shared == 0:
            query = """INSERT INTO presences (user_id, status)
                       VALUES ($1, $2);
                    """
            await self.bot.db.execute(query, user.id, None)

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

        await ctx.send(discord.utils.escape_markdown(content))

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
            return await ctx.send(":x: User has no recorded nicknames for this server")

        content = ""
        for nick in nicks:
            recorded_at = nick["recorded_at"]
            timedelta = datetime.datetime.utcnow() - recorded_at
            content += f"\n{nick['nick']} - {humanize.naturaldate(recorded_at)} ({humanize.naturaldelta(timedelta)} ago)"

        await ctx.send(discord.utils.escape_markdown(content))

    @commands.command(name="avatars", descripion="Avatars")
    async def avatars(self, ctx, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        await ctx.trigger_typing()

        query = """SELECT *
                   FROM avatars
                   WHERE avatars.user_id=$1
                   ORDER BY avatars.recorded_at DESC;
                """
        avatars = await self.bot.db.fetch(query, user.id)

        partial = functools.partial(self.draw_image, avatars)
        file = await self.bot.loop.run_in_executor(None, partial)
        file.seek(0)
        await ctx.send(content=f"Avatars for {user}", file=discord.File(fp=file, filename="image.png"))

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

    @commands.command(name="userinfo", description="Get info on a user", aliases=["ui", "whois"])
    async def userinfo(self, ctx, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        await ctx.trigger_typing()

        query = """SELECT *
                   FROM names
                   WHERE names.user_id=$1
                   ORDER BY names.recorded_at DESC;
                """
        names = await self.bot.db.fetch(query, user.id)

        query = """SELECT *
                   FROM nicks
                   WHERE nicks.user_id=$1 AND nicks.guild_id=$2
                   ORDER BY nicks.recorded_at DESC;
                """
        nicks = await self.bot.db.fetch(query, user.id, ctx.guild.id)

        query = """SELECT *
                   FROM avatars
                   WHERE avatars.user_id=$1
                   ORDER BY avatars.recorded_at DESC;
                """
        avatars = await self.bot.db.fetch(query, user.id)

        partial = functools.partial(self.draw_image, avatars)
        image = await self.bot.loop.run_in_executor(None, partial)
        image.seek(0)

        em = discord.Embed(title=f"{user.display_name} ({user.id})")
        em.set_image(url="attachment://avatars.png")
        em.set_thumbnail(url=user.avatar_url)

        em.add_field(name="Names", value=", ".join([name["name"] for name in names]))
        if nicks:
            em.add_field(name="Nicks", value=", ".join([nick["nick"] for nick in nicks]))
        em.add_field(name="Avatar Count", value=len(avatars))

        await ctx.send(embed=em, file=discord.File(image, filename="avatars.png"))

    @commands.command(name="pie", description="View a user's presence pie chart")
    async def pie(self, ctx, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        await ctx.trigger_typing()

        query = """SELECT *
                   FROM presences
                   WHERE presences.user_id=$1;
                """
        presences = await self.bot.db.fetch(query, user.id)

        settings = self.bot.get_cog("Settings")
        if settings:
            config = await settings.fetch_config(ctx.author.id)
            theme = config.theme if config else get_theme(None)

        else:
            theme = get_theme(None)

        file = io.BytesIO()
        partial = functools.partial(self.draw_pie, presences, theme)
        image = await self.bot.loop.run_in_executor(None, partial)
        image.save(file, "PNG")
        file.seek(0)

        await ctx.send(content=f"Pie chart for {user}", file=discord.File(file, filename="pie.png"))

    def draw_pie(self, presences, theme):
        presence_times = {"online": 0, "idle": 0, "dnd": 0, "offline": 0}
        for counter, presence in enumerate(presences):
            if presence["status"]:
                if len(presences) > counter+1:
                    next_time = presences[counter+1]["recorded_at"]
                else:
                    next_time = datetime.datetime.utcnow()
                time = next_time-presence["recorded_at"]
                presence_times[presence["status"]] = presence_times[presence["status"]]+time.total_seconds()

        total = sum(list(presence_times.values()))

        online = (presence_times["online"]/total)
        idle = (presence_times["idle"]/total)
        dnd = (presence_times["dnd"]/total)
        offline = (presence_times["offline"]/total)

        width = 2048
        height = 2048
        shape = [(500, 500), (2000, 2000)]

        image = Image.new("RGB", (width, height), theme.background)
        drawing = ImageDraw.Draw(image)
        drawing.pieslice(shape, start=0, end=round(online*360, 2), fill="green")
        drawing.pieslice(shape, start=round(online*360, 2), end=round((online+idle)*360, 2), fill="yellow")
        drawing.pieslice(shape, start=round((online+idle)*360, 2), end=round((online+idle+dnd)*360, 2), fill="red")
        drawing.pieslice(shape, start=round((online+idle+dnd)*360, 2), end=round(360, 2), fill="gray")


        text = f"Online - {round(online*100, 2) or 0}% \nIdle - {round(idle*100, 2) or 0}% \nDo Not Disturb - {round(dnd*100, 2) or 0}% \nOffline - {round(offline*100, 2) or 0}%"
        font = ImageFont.truetype("arial", 120)
        drawing.text(xy=(120, 0), text=text, fill=theme.primary, font=font, spacing=10)

        drawing.rectangle([(10, 10), (110, 120)], fill="green")
        drawing.rectangle([(10, 130), (110, 240)], fill="yellow")
        drawing.rectangle([(10, 250), (110, 360)], fill="red")
        drawing.rectangle([(10, 370), (110, 480)], fill="gray")

        return image

    @commands.command(name="chart", description="View a chart of your status over the past month")
    async def status(self, ctx, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        await ctx.trigger_typing()

        query = """SELECT *
                   FROM presences
                   WHERE presences.user_id=$1;
                """
        presences = await self.bot.db.fetch(query, user.id)

        settings = self.bot.get_cog("Settings")
        if settings:
            config = await settings.fetch_config(ctx.author.id)
            theme = config.theme if config else get_theme(None)

        else:
            theme = get_theme(None)

        file = io.BytesIO()
        partial = functools.partial(self.draw_chart, presences, theme)
        image = await self.bot.loop.run_in_executor(None, partial)
        image.save(file, "PNG")
        file.seek(0)

        await ctx.send(content=f"Status chart for {user}", file=discord.File(file, filename="chart.png"))

    def draw_chart(self, presences, theme):
        image = Image.new("RGB", (3480, 3200), theme.background)
        drawing = ImageDraw.Draw(image, "RGBA")
        font = ImageFont.truetype("arial", 100)

        time = datetime.datetime.utcnow()-datetime.timedelta(days=30)

        days = calendar.monthrange(time.year, time.month)[1]
        if days <= time.day:
            time = time.replace(month=time.month+1, day=(time.day - int(days))+1)
        else:
            time = time.replace(day=time.day+1)

        time = datetime.datetime(year=time.year, month=time.month, day=time.day)
        keys = {"online": "green", "idle": "yellow", "dnd": "red", "offline": "gray"}
        months = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

        for row in range(2, 32):
            for pixel in range(600, 3481):
                last = None
                color = None
                found = None
                for presence in presences:
                    if presence["recorded_at"] > time:
                        if last and last["status"]:
                            color = keys[last["status"]]
                        else:
                            color = None
                        found = True
                        break
                    last = presence

                if not found and time < datetime.datetime.utcnow():
                    color = keys[presences[-1]["status"]]

                if color:
                    drawing.rectangle([(pixel, row*100), (pixel+1, (row*100)+99)], fill=color)
                time += datetime.timedelta(seconds=30)

                days = calendar.monthrange(time.year, time.month)[1]
                if days < time.day:
                    time = time.replace(month=time.month+1, day=(time.day - int(days))+1)

            drawing.text(xy=(1, row*100), text=f"{(time-datetime.timedelta(days=1)).strftime('%A')[:3]}, {months[(time-datetime.timedelta(days=1)).month]} {(time-datetime.timedelta(days=1)).day}", fill=theme.primary, font=font)
            drawing.line(xy=[(1, row*100), (3480, row*100)], fill=theme.secondary, width=5)

        for hour in range(24):
            if hour%6 == 0:
                drawing.text(xy=((hour*120)+600, 1), text=f"{hour}:00 UTC", fill=theme.primary, font=font)
            drawing.line(xy=[((hour*120)+600, 200), ((hour*120)+600, 3500)], fill=theme.secondary, width=5)

        return image

def setup(bot):
    bot.add_cog(Tracking(bot))
