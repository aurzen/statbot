from __future__ import annotations

import asyncio

import TOKENS
import aurcore
import aurflux
from aurflux.argh import *
import stats
if ty.TYPE_CHECKING:
    from aurflux.command import *


class Statbot():
    def __init__(self):
        self.event_router = aurcore.event.EventRouter(name=self.__class__.__name__)
        self.flux = aurflux.Aurflux(self.__class__.__name__, admin_id=TOKENS.ADMIN_ID, parent_router=self.event_router)


    async def startup(self, token: str):
        await self.flux.startup(token)

    async def shutdown(self):
        await self.flux.logout()



roombot = Statbot()
roombot.flux.register_cog(stats.MessageScraper)

aurcore.aiorun(roombot.startup(token=TOKENS.STATBOT), roombot.shutdown())
