import asyncio
from asyncio import Event
from typing import Awaitable, Coroutine, Callable

from aiohttp import ClientSession, WSMsgType
from dsnet.core import Conversation, Query
from dsnet.crypto import gen_key_pair

from yarl import URL

from dsnetclient.repository import Repository
import logging


class DsnetApi:
    def __init__(self, url: URL, repository: Repository,
                 notification_cb: Callable[[bytes], Awaitable[None]] = None) -> None:
        self.repository = repository
        self.base_url = url
        self.client = ClientSession()
        self.notification_cb = notification_cb
        self.stop = False
        if notification_cb is not None:
            self._listener = asyncio.get_event_loop().create_task(self.notifications())

    async def get_server_version(self) -> dict:
        async with self.client.get(self.base_url) as resp:
            return await resp.json()

    async def send_query(self, query: str) -> None:
        query_keys = gen_key_pair()

        for peer in await self.repository.peers():
            conv = Conversation(query_keys.private, peer.public_key, query, querier=True)
            await self.repository.save_conversation(conv)

        payload = Query(query_keys.public, query).to_bytes()
        async with self.client.post(self.base_url.join(URL('/bb/broadcast')), data=payload) as response:
            response.raise_for_status()

    def close(self):
        self.stop = True

    async def notifications(self):
        async with ClientSession() as session:
            async with session.ws_connect(self.base_url.join(URL('/notifications'))) as ws:
                while not self.stop:
                    async for msg in ws:
                        if msg.type == WSMsgType.BINARY:
                            await self.notification_cb(msg.data)
                        elif msg.type == WSMsgType.TEXT:
                            await self.notification_cb(msg.data.encode("utf-8"))
                        else:
                            logging.warning(f"received unhandled type {msg.type}")


