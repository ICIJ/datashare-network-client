import asyncio
import logging
from asyncio import Task
from typing import Awaitable, Callable

import databases
from aiohttp import ClientSession, WSMsgType, ClientConnectorError
from dsnet.core import Conversation, Query
from dsnet.crypto import gen_key_pair, get_public_key
from dsnet.message import Message, MessageType
from sqlalchemy import create_engine
from yarl import URL

from dsnetclient.index import Index
from dsnetclient.models import metadata
from dsnetclient.repository import Repository, SqlalchemyRepository


class DsnetApi:
    def __init__(self, url: URL, repository: Repository, private_key: bytes, reconnect_delay_seconds=2, index: Index = None) -> None:
        self.repository = repository
        self.base_url = url
        self.index = index
        self.private_key = private_key
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.stop = False
        self.ws = None

    async def get_server_version(self) -> dict:
        async with ClientSession() as session:
            async with session.get(self.base_url) as resp:
                return await resp.json()

    async def send_query(self, query: bytes) -> None:
        query_keys = gen_key_pair()
        public_key = get_public_key(self.private_key)
        for peer in await self.repository.peers():
            if peer.public_key != public_key:
                conv = Conversation.create_from_querier(query_keys.private, peer.public_key, query)
                await self.repository.save_conversation(conv)

        payload = Query(query_keys.public, query).to_bytes()
        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL('/bb/broadcast')), data=payload) as response:
                response.raise_for_status()

    async def send_response(self, public_key: bytes, response_data: bytes):
        conv = Conversation.create_from_recipient(private_key=self.private_key, other_public_key=public_key)
        response = conv.create_response(response_data)
        await self.repository.save_conversation(conv)

        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL(f'/ph/{response.address.hex()}')), data=response.payload) as http_response:
                http_response.raise_for_status()

    async def close(self):
        self.stop = True
        if self.ws is not None: await self.ws.close()

    async def start_listening(self, notification_cb: Callable[[Message], Awaitable[None]] = None,
                              decoder: Callable[[bytes], Message] = MessageType.loads):
        callback = self.websocket_callback if notification_cb is None else notification_cb
        while not self.stop:
            self.ws = None
            try:
                async with ClientSession() as session:
                    async with session.ws_connect(self.base_url.join(URL('/notifications'))) as self.ws:
                        async for msg in self.ws:
                            if msg.type == WSMsgType.BINARY:
                                await callback(decoder(msg.data))
                            elif msg.type == WSMsgType.TEXT:
                                await callback(decoder(msg.data.encode()))
                            else:
                                logging.warning(f"received unhandled type {msg.type}")
            except ClientConnectorError:
                logging.warning(f"ws connection lost waiting {self.reconnect_delay_seconds}s "
                                f"before reconnect to {self.base_url.join(URL('/notifications'))}")
                await asyncio.sleep(self.reconnect_delay_seconds)

    def background_listening(self, notification_cb: Callable[[Message], Awaitable[None]] = None,
                             decoder: Callable[[bytes], Message] = MessageType.loads) -> Task:
        return asyncio.get_event_loop().create_task(self.start_listening(notification_cb, decoder))

    async def websocket_callback(self, message: Message) -> None:
        if message.type() == MessageType.NOTIFICATION:
            await self.handle_ph_notification(message)
        elif message.type() == MessageType.QUERY:
            await self.handle_query(message)
        elif message.type() == MessageType.MESSAGE:
            await self.handle_message(message)
        else:
            logging.warning(f"received unhandled type {message.type()}")

    async def handle_ph_notification(self, msg: Message) -> None:
        pass

    async def handle_query(self, msg: Query) -> None:
        results = await self.index.search(msg.payload)
        if results:
            await self.send_response(msg.public_key, b'\\'.join(results))

    async def handle_message(self, msg: Message) -> None:
        pass


def main():
    # for testing
    url = 'sqlite:///dsnet.db'
    engine = create_engine(url)
    metadata.create_all(engine)
    repository = SqlalchemyRepository(databases.Database(url))
    api = DsnetApi(URL('http://localhost:8000'), repository, )

    async def cb(payload: bytes):
        print(payload)

    asyncio.get_event_loop().run_until_complete(api.start_listening(cb))


if __name__ == '__main__':
    main()
