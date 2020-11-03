import discord
from discord.ext import commands

import logging

log = logging.getLogger("logger.tracking")

class Tracking(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_user_update(self, before, after):

        if before.name != after.name:
            query = """INSERT INTO names (user_id, name)
                       VALUSE ($1, $2);
                    """
            await self.bot.db.execute(query, after.id, after.name)

        if before.avatar != after.avatar:
            filename = f"{after.id}-{after.avatar}.png"
            await after.avatar_url_as(format="png").save(f"images/{filename}")

            query = """INSERT INTO avatars (user_id, filename, hash)
                        VALUES ($1, $2, $3);
                    """
            await self.bot.db.execute(query, after.id, filename, after.avatar)

    @commands.Cog.listener()
    async def on_member_update(self, before, after):

        if before.nick != after.nick:
            query = """INSERT INTO nicks (user_id, guild_id, nick)
                       VALUES ($1, $2, $3);
                    """
            await self.bot.db.execute(query, after.id, after.guild.id, after.nick)

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        log.info("Joined a new guild")

        log.info("Loading database")
        nicks = await self.bot.db.fetch("SELECT * FROM nicks;")

        log.info("Updating nicknames")
        for member in guild.members:
            member_nicks = [nick for nick in nicks if nick["user_id"] == member.id and nick["guild_id"] == member.guild.id]
            if member.nick and (not member_nicks or member_nicks[-1]["nick"] != member.nick):
                query = """INSERT INTO nicks (user_id, guild_id, nick)
                           VALUES ($1, $2, $3);
                        """
                await self.bot.db.execute(query, member.id, member.guild.id, member.nick)

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

        if user.avatar and (not user_avatars or user_avatars[-1]["hash"] != user.avatar):
            filename = f"{user.id}-{user.avatar}.png"
            await user.avatar_url_as(format="png").save(f"images/{filename}")

            query = """INSERT INTO avatars (user_id, filename, hash)
                        VALUES ($1, $2, $3);
                    """
            await self.db.execute(query, user.id, filename, user.avatar)

        if not user_names or user_names[-1]["name"] != user.name:
            query = """INSERT INTO names (user_id, name)
                        VALUES ($1, $2);
                    """
            await self.db.execute(query, user.id, user.name)

def setup(bot):
    bot.add_cog(Tracking(bot))
