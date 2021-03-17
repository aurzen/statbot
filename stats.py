from __future__ import annotations

import typing as ty

import asyncpg
import asyncpg.pool
import aurflux.auth
import aurflux.cog
import discord
import tqdm
from aurflux.command import Response
from aurflux.context import GuildMessageCtx

import TOKENS

if ty.TYPE_CHECKING:
   pass

import decompose
import aurflux
import discord.ext.commands
import pickle


class MessageScraper(aurflux.cog.FluxCog):

   def __init__(self, *args, **kwargs):
      self.pool: ty.Optional[asyncpg.pool.Pool] = None
      super(MessageScraper, self).__init__(*args, **kwargs)

   async def startup(self):
      print(f"Starting up {self}")
      self.pool: asyncpg.pool.Pool = await asyncpg.create_pool(TOKENS.PSQL_STRING, ssl=True)

   async def process_message(self, message: discord.Message):
      message_query, message_args = decompose.build_insert(decompose.message(message), "messages")
      async with self.pool.acquire() as c:
         await c.execute(message_query, *message_args)

      mention_query, mention_args = decompose.build_insert(decompose.mentions(message), "mentions")
      if mention_query:
         async with self.pool.acquire() as c:
            await c.execute(mention_query, *mention_args)

   async def oldest_in_channel(self, channel_id: int):
      async with self.pool.acquire() as c:
         oldest = await c.fetchrow("SELECT MIN(created_at) FROM messages WHERE channel_id = $1 AND emoji_ids != '{}'", channel_id)
         print(oldest)

      return oldest["min"]

   async def scrape_channel(self, channel: discord.TextChannel):
      oldest = None
      try:
         with open(f"{channel.id}.latest", "rb") as f:
            oldest = pickle.load(f)
      except FileNotFoundError:
         pass

      pbar = tqdm.tqdm()
      try:
         i: int = 0

         async for message in channel.history(limit=None, before=oldest):
            await self.process_message(message)
            pbar.update()
            if i % 1000 == 0:
               pbar.set_description(f"{channel.name}: {message.created_at.isoformat()}")
               i = 0
            if i % 10000 == 0:
               with open(f"{channel.id}.latest", "wb") as f:
                  pickle.dump(message.created_at, f)

            i += 1
      except discord.errors.Forbidden:
         pass

   def load(self):
      @self.flux.router.listen_for(":message")
      async def message_handler(message: discord.Message):
         await self.process_message(message)

      @self._commandeer(name="scrape")
      async def scrape(_: GuildMessageCtx, args):
         if channel := self.flux.get_channel(int(args)):
            await self.scrape_channel(channel)
         else:
            for channel in self.flux.get_guild(int(args)).text_channels:
               await self.scrape_channel(channel)
         return Response("Done!")

      @self._commandeer(name="names")
      async def names(_: GuildMessageCtx, _0):
         async with self.pool.acquire() as c:
            async with self.pool.acquire() as c2:
               async with c.transaction():
                  pbar = tqdm.tqdm()
                  async for record in c.cursor("SELECT DISTINCT author_id from messages"):
                     pbar.update(1)
                     user_id = record["author_id"]
                     user = await self.flux.fetch_user(user_id)
                     if user:
                        user_query, user_args = decompose.build_insert(decompose.user(user), "users")
                        await c2.execute(user_query, *user_args)

         return Response()

   def override_auths(self) -> ty.List[aurflux.auth.Record]:
      return [aurflux.auth.Record.deny_all()]
