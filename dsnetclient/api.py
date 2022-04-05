import asyncio
from asyncio import Task
from typing import Awaitable, Callable, Tuple, List

import databases
from aiohttp import ClientSession, WSMsgType, ClientConnectorError
from authlib.integrations.httpx_client import AsyncOAuth2Client
from dsnet.core import Conversation, Query
from dsnet.crypto import gen_key_pair, get_public_key
from dsnet.logger import logger
from dsnet.message import Message, MessageType, PigeonHoleNotification, PigeonHoleMessage
from dsnet.token import generate_tokens, generate_challenges
from sqlalchemy import create_engine
from sscred import (
    AbePublicKey,
    SignerCommitMessage,
    SignerResponseMessage,
    packb,
    unpackb,
)
from yarl import URL

from dsnetclient.index import Index, MemoryIndex
from dsnetclient.models import metadata
from dsnetclient.repository import Repository, SqlalchemyRepository


class NoTokenException(Exception):
    pass


class DsnetApi:
    def __init__(self, url: URL, repository: Repository, secret_key: bytes, reconnect_delay_seconds=2,
                 index: Index = None, oauth_client: AsyncOAuth2Client = None) -> None:
        self.repository = repository
        self.base_url = url
        self.index = index
        self.oauth_client = oauth_client
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
        abe_token = await self.repository.pop_token()
        if abe_token is None:
            raise NoTokenException()

        for peer in await self.repository.peers():
            conv = Conversation.create_from_querier(query_keys.secret, peer.public_key, query)
            await self.repository.save_conversation(conv)

        payload = Query.create(query_keys.public, abe_token, query).to_bytes()
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
        nb_errors = 0
        nb_max_errors = 5
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
                nb_errors += 1
                logger.exception(e)
                if nb_errors >= nb_max_errors:
                    raise e

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
        server_key: AbePublicKey = await self.repository.get_token_server_key()
        if msg.validate(server_key):
            results = await self.index.search(msg.payload)
            await self.send_response(msg.public_key, results)
        else:
            logger.warning(f"invalid query's signature {msg.public_key.hex()}")

    def start_auth(self, authorize_url: str) -> Tuple[str, str]:
        return self.oauth_client.create_authorization_url(authorize_url)

    async def end_auth(self, token_endpoint: str, authorization_response: str):
        return await self.oauth_client.fetch_token(token_endpoint, authorization_response=authorization_response)

    async def show_tokens(self) -> List[bytes]:
        return [packb(abe_token.token) for abe_token in await self.repository.get_tokens()]

    async def fetch_pre_tokens(self) -> int:
        publickey_resp = await self.oauth_client.get('/api/v2/dstokens/publickey')
        server_public_key_raw = publickey_resp.content
        local_key = await self.repository.get_token_server_key()

        server_key: AbePublicKey = unpackb(server_public_key_raw)
        if server_key != local_key:
            commitments_resp = await self.oauth_client.post('/api/v2/dstokens/commitments')
            commitments: List[SignerCommitMessage] = unpackb(commitments_resp.content)

            challenges, challenges_internal, token_secret_keys = generate_challenges(server_key, commitments)

            pretoken_resp = await self.oauth_client.post(
                '/api/v2/dstokens/pretokens',
                headers={'Content-Type': 'application/x-msgpack'},
                content=packb(challenges)
            )
            pretokens: List[SignerResponseMessage] = unpackb(pretoken_resp.content)
            tokens = generate_tokens(server_key, challenges_internal, token_secret_keys, pretokens)

            # bulk insert tokens in DB
            await self.repository.save_token_server_key(server_key)
            await self.repository.save_tokens(tokens)

            return len(tokens)
        return 0


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
