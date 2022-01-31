import asyncio
from asyncio import Event, Task
from typing import Awaitable, Coroutine, Callable

from aiohttp import ClientSession, WSMsgType, ClientConnectorError
from dsnet.core import Conversation, Query
from dsnet.crypto import gen_key_pair

from yarl import URL

from dsnetclient.repository import Repository
import logging


class DsnetApi:
    def __init__(self, url: URL, repository: Repository, reconnect_delay_seconds=2) -> None:
        self.repository = repository
        self.base_url = url
        self.client = ClientSession()
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.stop = False
        self.ws = None

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

    async def close(self):
        self.stop = True
        await self.client.close()
        if self.ws is not None: await self.ws.close()

    async def start_listening(self, notification_cb: Callable[[bytes], Awaitable[None]] = None):
        while not self.stop:
            self.ws = None
            try:
                async with ClientSession() as session:
                    async with session.ws_connect(self.base_url.join(URL('/notifications'))) as self.ws:
                        async for msg in self.ws:
                            if msg.type == WSMsgType.BINARY:
                                await notification_cb(msg.data)
                            elif msg.type == WSMsgType.TEXT:
                                await notification_cb(msg.data.encode("utf-8"))
                            else:
                                logging.warning(f"received unhandled type {msg.type}")
            except ClientConnectorError:
                logging.warning(f"client connection waiting {self.reconnect_delay_seconds} before reconnect")
                await asyncio.sleep(self.reconnect_delay_seconds)

    def background_listening(self, notification_cb: Callable[[bytes], Awaitable[None]] = None) -> Task:
        return asyncio.get_event_loop().create_task(self.start_listening(notification_cb))
