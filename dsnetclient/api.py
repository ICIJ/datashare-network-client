import asyncio
from asyncio import Task
from typing import Awaitable, Callable

import databases
from aiohttp import ClientSession, WSMsgType, ClientConnectorError
from dsnet.core import Conversation, Query
from dsnet.crypto import gen_key_pair, get_public_key
from dsnet.logger import logger
from dsnet.message import Message, MessageType, PigeonHoleNotification, PigeonHoleMessage
from sqlalchemy import create_engine
from yarl import URL

from dsnetclient.index import Index, MemoryIndex
from dsnetclient.models import metadata
from dsnetclient.repository import Repository, SqlalchemyRepository


class DsnetApi:
    def __init__(self, url: URL, repository: Repository, secret_key: bytes, reconnect_delay_seconds=2, index: Index = None) -> None:
        self.repository = repository
        self.base_url = url
        self.index = index
        self.secret_key = secret_key
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.stop = False
        self.ws = None

    async def get_server_version(self) -> dict:
        async with ClientSession() as session:
            async with session.get(self.base_url) as resp:
                return await resp.json()

    async def send_query(self, query: bytes) -> None:
        query_keys = gen_key_pair()
        public_key = get_public_key(self.secret_key)
        for peer in await self.repository.peers():
            if peer.public_key != public_key:
                conv = Conversation.create_from_querier(query_keys.secret, peer.public_key, query)
                await self.repository.save_conversation(conv)

        payload = Query(query_keys.public, query).to_bytes()
        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL('/bb/broadcast')), data=payload) as response:
                response.raise_for_status()

    async def send_response(self, public_key: bytes, response_data: bytes) -> None:
        conv = Conversation.create_from_recipient(secret_key=self.secret_key, other_public_key=public_key)
        await self._send_message(conv, response_data)

    async def send_message(self, conversation_id: int, message: bytes) -> None:
        conv = await self.repository.get_conversation(conversation_id)
        await self._send_message(conv, message)

    async def _send_message(self, conversation: Conversation, message: bytes) -> None:
        response = conversation.create_response(message)
        await self.repository.save_conversation(conversation)

        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL(f'/ph/{response.address.hex()}')), data=response.to_bytes()) as http_response:
                http_response.raise_for_status()

    async def close(self):
        self.stop = True
        if self.ws is not None: await self.ws.close()

    async def start_listening(self, notification_cb: Callable[[Message], Awaitable[None]] = None,
                              decoder: Callable[[bytes], Message] = MessageType.loads):
        callback = self.websocket_callback if notification_cb is None else notification_cb
        url_ws = self.base_url.join(URL('/notifications'))
        while not self.stop:
            self.ws = None
            try:
                async with ClientSession() as session:
                    async with session.ws_connect(url_ws) as self.ws:
                        logger.info(f"connected to websocket {url_ws}")
                        async for msg in self.ws:
                            if msg.type == WSMsgType.BINARY:
                                await callback(decoder(msg.data))
                            elif msg.type == WSMsgType.TEXT:
                                await callback(decoder(msg.data.encode()))
                            else:
                                logger.warning(f"received unhandled type {msg.type}")
            except ClientConnectorError:
                logger.warning(f"ws connection lost waiting {self.reconnect_delay_seconds}s "
                               f"before reconnect to {url_ws}")
                await asyncio.sleep(self.reconnect_delay_seconds)
            except Exception as e:
                logger.error(e)

    def background_listening(self, notification_cb: Callable[[Message], Awaitable[None]] = None,
                             decoder: Callable[[bytes], Message] = MessageType.loads) -> Task:
        return asyncio.get_event_loop().create_task(self.start_listening(notification_cb, decoder))

    async def websocket_callback(self, message: Message) -> None:
        logger.debug(f"received message type {message.type()}")
        if message.type() == MessageType.NOTIFICATION:
            await self.handle_ph_notification(message)
        elif message.type() == MessageType.QUERY:
            await self.handle_query(message)
        else:
            logger.warning(f"received unhandled type {message.type()}")

    async def handle_ph_notification(self, msg: PigeonHoleNotification) -> None:
        logger.info(f"received ph notification for {msg.adr_hex}")
        async with ClientSession() as session:
            for ph in await self.repository.get_pigeonholes_by_adr(msg.adr_hex):
                async with session.get(self.base_url.join(URL(f'/ph/{ph.address.hex()}'))) as http_response:
                    http_response.raise_for_status()
                    message = PigeonHoleMessage.from_bytes(await http_response.read())
                    message.from_key = ph.key_for_hash
                    conversation = await self.repository.get_conversation_by_address(ph.address)
                    logger.debug(f"adding message {message.address.hex()} to conversation {conversation.id}")
                    conversation.add_message(message)
                    await self.repository.save_conversation(conversation)

    async def handle_query(self, msg: Query) -> None:
        logger.info(f"received query {msg.public_key.hex()}")
        local_query = await self.repository.get_conversation_by_key(msg.public_key)
        if local_query is None:
            results = await self.index.search(msg.payload)
            if results:
                await self.send_response(msg.public_key, b'\\'.join(results))


def main():
    # for testing
    keys = gen_key_pair()
    url = 'sqlite:///dsnet.db'
    engine = create_engine(url)
    metadata.create_all(engine)
    repository = SqlalchemyRepository(databases.Database(url))
    api = DsnetApi(URL('http://localhost:8000'), repository, keys.secret, index=MemoryIndex({"foo", "bar"}))

    asyncio.get_event_loop().run_until_complete(api.start_listening())


if __name__ == '__main__':
    main()
