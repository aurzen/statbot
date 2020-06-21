from __future__ import annotations

import asyncio as aio
import aurcore
import asyncpg
import asyncpg.pool
import aurflux
import typing as ty
import TOKENS
import tqdm
import discord

if ty.TYPE_CHECKING:
    from aurflux.command import *

import decompose
import aurflux


class MessageScraper(aurflux.AurfluxCog):
    async def startup(self):
        # noinspection PyAttributeOutsideInit
        self.pool: asyncpg.pool.Pool = await asyncpg.create_pool(TOKENS.PSQL_STRING, ssl=True)
        # self.conn: asyncpg.Connection = await asyncpg.connect(TOKENS.PSQL_STRING, ssl=True)
        # await self.conn.execute('CREATE UNLOGGED TABLE IF NOT EXISTS mentions('
        #                         'message_id BIGINT PRIMARY KEY NOT NULL,'
        #                         'author_id BIGINT NOT NULL,'
        #                         'target_id BIGINT NOT NULL,'
        #                         'type TEXT NOT NULL)')
        # await self.conn.execute('CREATE UNLOGGED TABLE IF NOT EXISTS messages('
        #                         'message_id BIGINT PRIMARY KEY UNIQUE NOT NULL,'
        #                         'author_id BIGINT NOT NULL,'
        #                         'guild_id BIGINT,'
        #                         'created_at TIMESTAMPTZ NOT NULL,'
        #                         'channel_id BIGINT NOT NULL,'
        #                         'content TEXT,'
        #                         'clean_content TEXT,'
        #                         'has_mentions BOOLEAN NOT NULL DEFAULT FALSE,'
        #                         'embeds TEXT)')

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
            oldest = await c.fetchrow("SELECT created_at FROM messages WHERE channel_id = $0 ORDER BY created_at LIMIT 1", channel_id)
        return oldest["created_at"]

    def route(self):
        @self.aurflux.router.endpoint(":message", decompose=True)
        async def message_handler(message: discord.Message):
            await self.process_message(message)

        @self.aurflux.commandeer(name="scrape", parsed=False, private=True)
        async def scrape(ctx: MessageContext, args):
            pbar = tqdm.tqdm()
            channel: discord.TextChannel
            for channel in self.aurflux.get_guild(int(args)).text_channels:
                try:
                    async for message in channel.history(limit=None, before=await self.oldest_in_channel(channel.id)):
                        await self.process_message(message)
                        pbar.update()
                except discord.errors.Forbidden:
                    pass
