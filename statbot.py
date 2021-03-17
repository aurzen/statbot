from __future__ import annotations

import typing as ty

import TOKENS
import aurcore
import aurflux
import stats
import discord

if ty.TYPE_CHECKING:
    from aurflux.command import *


class Statbot:
    def __init__(self):
        self.event_router = aurcore.event.EventRouterHost(name=self.__class__.__name__)
        self.flux = aurflux.FluxClient(self.__class__.__name__, admin_id=TOKENS.ADMIN_ID, parent_router=self.event_router, intents=discord.Intents.all())

    async def startup(self, token: str):
        await self.flux.startup(token)

    async def shutdown(self):
        await self.flux.logout()


roombot = Statbot()
roombot.flux.register_cog(stats.MessageScraper)

aurcore.aiorun(roombot.startup(token=TOKENS.STATBOT), roombot.shutdown())
