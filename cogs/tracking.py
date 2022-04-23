from __future__ import annotations

import asyncio
import calendar
import datetime
import functools
import io
import logging
import typing

import asyncpg
import discord
import humanize
from discord.ext import commands, tasks
from PIL import Image, ImageDraw, ImageFont, ImageOps

from .utils import formats
from .utils.context import Context
from .utils.theme import get_theme

if typing.TYPE_CHECKING:
    from bot import Logger

log = logging.getLogger("logger.tracking")

class MonthConverter(commands.Converter):
    async def convert(self, ctx, arg):
        months_mapping = {
            "Jan": 1,
            "January": 1,
            "Feb": 2,
            "Februrary": 3,
            "Mar": 3,
            "March": 3,
            "Apr": 4,
            "April": 4,
            "May": 5,
            "Jun": 6,
            "June": 6,
            "Jul": 7,
            "July": 7,
            "Aug": 8,
            "August": 8,
            "Sep": 9,
            "September": 9,
            "Oct": 10,
            "October": 10,
            "Nov": 11,
            "November": 11,
            "Dec": 12,
            "December": 12
        }

        if arg.isdigit():
            arg = int(arg)

            if arg > 0 and arg < 12:
                month = arg
            else:
                raise commands.BadArgument(f"Month {arg} is out of range")
        elif arg in months_mapping:
            month = months_mapping[arg]
        else:
            raise commands.BadArgument(f"Month {arg} not recognized")

        return month

class YearConverter(commands.Converter):
    async def convert(self, ctx, arg):
        if arg.isdigit():
            if len(arg) > 4:
                raise commands.BadArgument(f"Year {arg} is too long")

            arg = int(arg)
            year = arg
        else:
            raise commands.BadArgument(f"Year {arg} not recognized")

        return year


class Tracking(commands.Cog):
    def __init__(self, bot: Logger):
        self.bot = bot
        self._avatar_batch = []
        self._name_batch = []
        self._nick_batch = []
        self._presence_batch = []
        self._batch_lock = asyncio.Lock(loop=bot.loop)

    async def cog_load(self):
        self.bulk_insert_loop.add_exception_type(asyncpg.PostgresConnectionError)
        self.bulk_insert_loop.start()

    async def cog_unload(self):
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
            log.info("Registered %s to the database", format(formats.plural(total), "avatar"))
            self._avatar_batch.clear()

        query = """INSERT INTO names (user_id, name)
                   SELECT x.user_id, x.name
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, name TEXT)
                """
        if self._name_batch:
            await self.bot.db.execute(query, self._name_batch)
            total = len(self._name_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "name"))
            self._name_batch.clear()

        query = """INSERT INTO nicks (user_id, guild_id, nick)
                   SELECT x.user_id, x.guild_id, x.nick
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, guild_id BIGINT, nick TEXT)
                """
        if self._nick_batch:
            await self.bot.db.execute(query, self._nick_batch)
            total = len(self._nick_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "nick"))
            self._nick_batch.clear()

        query = """INSERT INTO presences (user_id, status)
                   SELECT x.user_id, x.status
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, status TEXT)
                """
        if self._presence_batch:
            await self.bot.db.execute(query, self._presence_batch)
            total = len(self._presence_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "presence"))
            self._presence_batch.clear()

    @tasks.loop(seconds=20.0)
    async def bulk_insert_loop(self):
        async with self._batch_lock:
            await self.bulk_insert()

    @bulk_insert_loop.before_loop
    async def before_bulk_insert_loop(self):
        log.info("Waiting until the database is ready to start the bulk insert loop")
        await self.bot.wait_until_db_ready()
        log.info("Starting bulk insert loop")

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        log.info("Joined a new guild")

        log.info("Loading database")
        nicks = await self.bot.db.fetch("SELECT * FROM nicks;")
        presences = await self.bot.db.fetch("SELECT * FROM presences;")

        log.info("Loading members")
        members = [discord.Member._copy(member) for member in guild.members]

        nick_batch = []
        presence_batch = []

        log.info("Querying nicks and presences")
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
            log.info("Registered %s to the database", format(formats.plural(total), "nick"))
        else:
            log.info("No work needed for nicks")

        query = """INSERT INTO presences (user_id, status)
                   SELECT x.user_id, x.status
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(user_id BIGINT, status TEXT)
                """
        if presence_batch:
            await self.bot.db.execute(query, presence_batch)
            total = len(presence_batch)
            log.info("Registered %s to the database", format(formats.plural(total), "presence"))
        else:
            log.info("No work needed for presences")

        log.info("Querying avatars and names")
        await self.bot.update_users(members)

    @commands.Cog.listener()
    async def on_member_join(self, user: discord.Member):
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

        avatar_key = user.avatar.key if user.avatar else None
        if not user_avatars or user_avatars[-1]["hash"] != avatar_key:
            if user.avatar:
                try:
                    filename = f"{user.id}-{user.avatar.key}.png"
                    await user.avatar.with_format("png").save(f"images/{filename}")

                    query = """INSERT INTO avatars (user_id, filename, hash)
                                VALUES ($1, $2, $3);
                            """
                    await self.bot.db.execute(query, user.id, filename, user.avatar.key)
                except discord.NotFound:
                    log.warning(f"Failed to fetch avatar for {user} ({user.id}). Ignoring")

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
    async def on_user_update(self, before: discord.User, after: discord.User):
        if before.name != after.name:
            self._name_batch.append({"user_id": after.id, "name": after.name})

        before_avatar_key = before.avatar.key if before.avatar else None
        after_avatar_key = after.avatar.key if after.avatar else None
        if before_avatar_key != after_avatar_key:
            if after.avatar:
                filename = f"{after.id}-{after.avatar.key}.png"
                await after.display_avatar.with_format("png").save(f"images/{filename}")

                self._avatar_batch.append(
                    {"user_id": after.id, "filename": filename, "hash": after.avatar.key}
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
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if after.nick and before.nick != after.nick:
            self._nick_batch.append(
                {
                    "user_id": after.id,
                    "guild_id": after.guild.id,
                    "nick": after.nick,
                }
            )

        presences = [presence for presence in self._presence_batch if presence["user_id"] == after.id]
        if (not presences or presences[-1]["status"] != str(after.status)) and str(before.status) != str(after.status):
            self._presence_batch.append(
                {
                    "user_id": after.id,
                    "status": str(after.status)
                }
            )

    @commands.Cog.listener()
    async def on_member_remove(self, user: discord.Member):
        if not [guild for guild in self.bot.guilds if user.id in [member.id for member in guild.members]]:
            query = """INSERT INTO presences (user_id, status)
                       VALUES ($1, $2);
                    """
            await self.bot.db.execute(query, user.id, None)

    @commands.hybrid_command(name="names", description="View past usernames for a user")
    async def names(self, ctx: Context, *, user: discord.Member = None):  # type: ignore
        if not user:
            user = ctx.author  # type: ignore

        query = """SELECT *
                   FROM names
                   WHERE names.user_id=$1
                   ORDER BY names.recorded_at DESC;
                """
        names = await self.bot.db.fetch(query, user.id)

        paginator = commands.Paginator(prefix=None, suffix=None)

        for name in names:
            recorded_at = name["recorded_at"]
            timedelta = datetime.datetime.utcnow() - recorded_at
            line = f"{name['name']} - {humanize.naturaldate(recorded_at)} ({humanize.naturaldelta(timedelta)} ago)"
            paginator.add_line(discord.utils.escape_markdown(line))

        for page in paginator.pages:
            await ctx.send(page)

    @commands.hybrid_command(name="nicks", description="View past nicknames for a user")
    async def nicks(self, ctx: Context, *, user: discord.Member = None):  # type: ignore
        if not user:
            user = ctx.author  # type: ignore

        query = """SELECT *
                   FROM nicks
                   WHERE nicks.user_id=$1 AND nicks.guild_id=$2
                   ORDER BY nicks.recorded_at DESC;
                """
        nicks = await self.bot.db.fetch(query, user.id, ctx.guild.id)

        if not nicks:
            return await ctx.send(":x: User has no recorded nicknames for this server")

        paginator = commands.Paginator(prefix=None, suffix=None)

        for nick in nicks:
            recorded_at = nick["recorded_at"]
            timedelta = datetime.datetime.utcnow() - recorded_at
            line = f"{nick['nick']} - {humanize.naturaldate(recorded_at)} ({humanize.naturaldelta(timedelta)} ago)"
            paginator.add_line(discord.utils.escape_markdown(line))

        for page in paginator.pages:
            await ctx.send(page)

    @commands.hybrid_command(name="avatars")
    async def avatars(self, ctx: Context, *, user: discord.Member = None):  # type: ignore
        """View past avatars for a user"""

        if not user:
            user = ctx.author  # type: ignore

        await ctx.defer()

        async with ctx.maybe_typing():
            query = """SELECT *
                       FROM avatars
                       WHERE avatars.user_id=$1
                       ORDER BY avatars.recorded_at DESC;
                    """
            avatars = await self.bot.db.fetch(query, user.id)

            partial = functools.partial(self.draw_avatars, avatars)
            file = await self.bot.loop.run_in_executor(None, partial)
            file.seek(0)

        await ctx.send(content=f"Avatars for {user}", file=discord.File(fp=file, filename="image.png"))

    def draw_avatars(self, avatars):
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

    @commands.hybrid_command(name="avatar", description="View a specific avatar in history")
    async def avatar(self, ctx: Context, user: typing.Optional[discord.Member], avatar: int = 1):
        if not user:
            user = ctx.author
        if avatar >= 0:
            avatar -= 1

        query = """SELECT *
                   FROM avatars
                   WHERE avatars.user_id=$1
                   ORDER BY avatars.recorded_at DESC;
                """
        avatars = await self.bot.db.fetch(query, user.id)

        try:
            avatar = avatars[avatar]
        except IndexError:
            return await ctx.send(":x: That is not a valid avatar")

        em = discord.Embed(timestamp=avatar["recorded_at"])
        em.set_author(name=user.display_name, icon_url=user.display_avatar.url)
        em.set_image(url="attachment://image.png")
        em.set_footer(text="Recorded")

        await ctx.send(content=f"Hash: {avatar['hash']}", embed=em, file=discord.File(f"images/{avatar['filename']}", filename="image.png"))

    @commands.hybrid_command(name="pie", description="View a user's presence pie chart")
    async def pie(self, ctx: Context, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        await ctx.defer()

        async with ctx.maybe_typing():
            query = """SELECT *
                       FROM presences
                       WHERE presences.user_id=$1
                       ORDER BY presences.recorded_at ASC;
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

    @commands.hybrid_command(name="ring", description="View a user's presence ring", aliases=["avatarpie"])
    async def ring(self, ctx: Context, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        await ctx.defer()

        async with ctx.maybe_typing():
            query = """SELECT *
                       FROM presences
                       WHERE presences.user_id=$1
                       ORDER BY presences.recorded_at ASC;
                    """
            presences = await self.bot.db.fetch(query, user.id)

            settings = self.bot.get_cog("Settings")
            if settings:
                config = await settings.fetch_config(ctx.author.id)
                theme = config.theme if config else get_theme(None)

            else:
                theme = get_theme(None)

            async with self.bot.session.get(str(user.display_avatar.with_format("png").url)) as resp:
                avatar = io.BytesIO(await resp.read())
                avatar = Image.open(avatar)

            file = io.BytesIO()
            partial = functools.partial(self.draw_pie, presences, theme, avatar)
            image = await self.bot.loop.run_in_executor(None, partial)
            image.save(file, "PNG")
            file.seek(0)

        await ctx.send(content=f"Pie chart for {user}", file=discord.File(file, filename="pie.png"))

    def draw_pie(self, presences, theme, avatar=None):
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

        if avatar:
            avatar_size = 1000
            shape_center = shape[0][0]+((shape[1][0]-shape[0][0])/2)
            avatar_center = avatar_size/2
            avatar_start = int(shape_center-avatar_center)

            mask = Image.new("L", (avatar_size, avatar_size), 0)
            draw = ImageDraw.Draw(mask)
            draw.ellipse((0, 0) + (avatar_size, avatar_size), fill=255)

            rounded_avatar = ImageOps.fit(avatar, mask.size, centering=(0.5, 0.5))
            rounded_avatar.putalpha(mask)

            image.paste(rounded_avatar, (avatar_start, avatar_start), rounded_avatar)

        return image

    @commands.hybrid_command(name="chart", description="View a chart of your status over the past month")
    async def chart(self, ctx: Context, *, user: discord.Member = None):
        if not user:
            user = ctx.author

        await ctx.defer()

        async with ctx.maybe_typing():
            query = """SELECT *
                       FROM presences
                       WHERE presences.user_id=$1
                       ORDER BY presences.recorded_at ASC;
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

        await ctx.send(content=f"Status chart for {user} during the past 30 days", file=discord.File(file, filename="chart.png"))

    @commands.hybrid_command(name="chartfor", description="View a chart of your status for a specified time period")
    async def chart_for(
        self,
        ctx: Context,
        month: typing.Optional[MonthConverter] = None,
        year: typing.Optional[YearConverter] = None,
        *,
        user: discord.Member = None,  # type: ignore
    ) -> None:
        if not user:
            user = ctx.author  # type: ignore

        if not month and not year:
            raise commands.BadArgument("Neither year nor month provided")

        now = datetime.datetime.utcnow()
        year = year or now.year
        month = month or now.month

        time = now.replace(year=year, month=month, day=1)-datetime.timedelta(days=1)

        await ctx.defer()

        async with ctx.maybe_typing():
            query = """SELECT *
                       FROM presences
                       WHERE presences.user_id=$1
                       ORDER BY presences.recorded_at ASC;
                    """
            presences = await self.bot.db.fetch(query, user.id)

            settings = self.bot.get_cog("Settings")
            if settings:
                config = await settings.fetch_config(ctx.author.id)
                theme = config.theme if config else get_theme(None)

            else:
                theme = get_theme(None)

            file = io.BytesIO()
            partial = functools.partial(self.draw_chart, presences, theme, time)
            image = await self.bot.loop.run_in_executor(None, partial)
            image.save(file, "PNG")
            file.seek(0)

        await ctx.send(content=f"Status chart for {user} in {(time+datetime.timedelta(days=1)).year}", file=discord.File(file, filename="chart.png"))

    def draw_chart(self, presences, theme, time=None):
        image = Image.new("RGB", (3480, 3200), theme.background)
        drawing = ImageDraw.Draw(image, "RGBA")
        font = ImageFont.truetype("arial", 100)

        if not time:
            time = datetime.datetime.utcnow()-datetime.timedelta(days=30)

        days = calendar.monthrange(time.year, time.month)[1]
        if days <= time.day:
            if time.month == 12:
                time = time.replace(year=time.year+1, month=1, day=(time.day - int(days))+1)
            else:
                time = time.replace(month=time.month+1, day=(time.day - int(days))+1)
        else:
            time = time.replace(day=time.day+1)

        time = datetime.datetime(year=time.year, month=time.month, day=time.day)
        keys = {"online": "green", "idle": "yellow", "dnd": "red", "offline": "gray"}
        months = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

        end_color = keys[presences[-1]["status"]]
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
                    del presences[:presences.index(last)]

                if not found and time < datetime.datetime.utcnow():
                    color = end_color

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

async def setup(bot: Logger) -> None:
    await bot.add_cog(Tracking(bot))
