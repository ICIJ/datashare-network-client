import asyncio
import datetime
import uuid
from asyncio import Task, AbstractEventLoop
from typing import Awaitable, Callable, Tuple, List, Optional

import databases
from aiohttp import ClientSession, WSMsgType, ClientConnectorError
from dsnet.core import Conversation, Query, QueryType
from dsnet.crypto import gen_key_pair
from dsnet.logger import logger
from dsnet.message import Message, MessageType, PigeonHoleNotification, PublicationMessage, PigeonHoleMessage
from dsnet.mspsi import MSPSIDocumentOwner, MSPSIQuerier, Document
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

from dsnetclient.index import Index, MemoryIndex, NamedEntity, NamedEntityCategory
from dsnetclient.message_retriever import MessageRetriever, AddressMatchMessageRetriever
from dsnetclient.message_sender import MessageSender, DirectMessageSender
from dsnetclient.models import metadata
from dsnetclient.repository import Repository, SqlalchemyRepository, Publication


class NoTokenException(Exception): pass
class InvalidAuthorizationResponse(Exception): pass


class DsnetApi:
    def __init__(
            self,
            url: URL,
            token_url: URL,
            repository: Repository,
            secret_key: bytes,
            message_retriever: MessageRetriever,
            message_sender: MessageSender,
            query_type: QueryType,
            reconnect_delay_seconds=2,
            index: Index = None,
    ) -> None:
        self.token_url = token_url
        self.repository = repository
        self.base_url = url
        self.index = index
        self.secret_key = secret_key
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.stop = False
        self.ws = None
        self.message_retriever = message_retriever
        self.message_sender = message_sender
        self.query_type = query_type

    async def get_server_version(self) -> dict:
        async with ClientSession() as session:
            async with session.get(self.base_url) as resp:
                return await resp.json()

    async def send_query(self, query: bytes) -> None:
        query_keys = gen_key_pair()
        abe_token = await self.repository.pop_token()
        if abe_token is None:
            raise NoTokenException()

        peers = await self.repository.peers()
        for idx, peer in enumerate(peers):
            mspsi_key = None if self.query_type == QueryType.CLEARTEXT else MSPSIQuerier.gen_key()
            conv = Conversation.create_from_querier(query_keys.secret, peer.public_key, query, query_mspsi_secret=mspsi_key)
            await self.repository.save_conversation(conv)
            if idx == len(peers) - 1:
                query_msg = conv.create_query(abe_token)
                async with ClientSession() as session:
                    async with session.post(self.base_url.join(URL('/bb/broadcast')), data=query_msg.to_bytes()) as response:
                        response.raise_for_status()

    async def send_response(self, public_key: bytes, response_data: bytes) -> None:
        conversation = Conversation.create_from_recipient(secret_key=self.secret_key, other_public_key=public_key)
        response = conversation.create_response(response_data)
        await self.repository.save_conversation(conversation)
        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL(f'/ph/{response.address.hex()}')), data=response.to_bytes()) as http_response:
                http_response.raise_for_status()

    async def send_message(self, conversation_id: int, message: bytes) -> None:
        conversation = await self.repository.get_conversation(conversation_id)
        response = conversation.create_response(message)
        await self.repository.save_conversation(conversation)
        await self.message_sender.send(response)

    async def close(self):
        self.stop = True
        if self.ws is not None: await self.ws.close()

    async def start_listening(self, notification_cb: Callable[[Message], Awaitable[None]] = None,
                              decoder: Callable[[bytes], Message] = MessageType.loads):
        callback = self.websocket_callback if notification_cb is None else notification_cb
        last_message_ts = await self.repository.get_last_broadcast_timestamp()
        notification_url = '/notifications' if last_message_ts is None else f'/notifications?ts={last_message_ts.timestamp()}'
        url_ws = self.base_url.join(URL(notification_url))
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
                             decoder: Callable[[bytes], Message] = MessageType.loads,
                             loop: Optional[AbstractEventLoop] = None) -> Task:
        evt_loop = asyncio.get_event_loop() if loop is None else loop
        return evt_loop.create_task(self.start_listening(notification_cb, decoder))

    async def websocket_callback(self, message: Message) -> None:
        logger.debug(f"received message type {message.type()}")
        if message.type() == MessageType.NOTIFICATION:
            await self.handle_ph_notification(message)
        elif message.type() == MessageType.QUERY:
            await self.handle_query(message)
        elif message.type() == MessageType.PUBLICATION:
            await self.handle_publication(message)
        else:
            logger.warning(f"received unhandled type {message.type()}")

    async def handle_ph_notification(self, msg: PigeonHoleNotification) -> None:
        logger.debug(f"received ph notification for {msg.adr_hex}")
        encoded_message_ph_tuple = await self.message_retriever.retrieve(msg)

        if encoded_message_ph_tuple is not None:
            encoded_message, ph = encoded_message_ph_tuple

            message = PigeonHoleMessage.from_bytes(encoded_message)
            message.from_key = ph.key_for_hash
            logger.info(f"received message {MessageType(message.type()).__repr__()}")

            conversation = await self.repository.get_conversation_by_address(message.address)
            if message.type() == MessageType.RESPONSE:
                payload = ph.decrypt(message.payload)
                results = await self.index.process_search_results(payload)
                conversation.add_results(results, ph)
            else:
                conversation.add_message(message)

            await self.repository.save_conversation(conversation)
        else:
            logger.debug("no message retrieved, ignoring")

    async def handle_query(self, msg: Query) -> None:
        logger.info(f"received query {msg.public_key.hex()}")
        server_key: AbePublicKey = await self.repository.get_token_server_key()
        if msg.validate(server_key):
            results = await self.index.search(msg.payload)
            await self.send_response(msg.public_key, results)
        else:
            logger.warning(f"invalid query's signature {msg.public_key.hex()}")

    async def handle_publication(self, msg: PublicationMessage):
        logger.info(f"received publication {msg.public_key.hex()}")
        await self.repository.save_publication_message(msg)

    async def show_tokens(self) -> List[bytes]:
        return [packb(abe_token.token) for abe_token in await self.repository.get_tokens()]

    async def fetch_pre_tokens(self, username: str, password: str, form_parser: Callable[[bytes, str, str], Tuple[str, dict]]) -> int:
        async with ClientSession() as session:
            publickey_resp = await session.get(self.token_url.join(URL('publickey')))
            server_public_key_raw = await publickey_resp.content.read()
            local_key = await self.repository.get_token_server_key()

            server_key: AbePublicKey = unpackb(server_public_key_raw)
            if server_key != local_key:
                commitments_resp = await session.post(self.token_url.join(URL('commitments')))

                content_type = commitments_resp.headers.get("Content-Type")
                if content_type != "application/x-msgpack":
                    if "text/html" not in content_type or username is None or password is None:
                        raise InvalidAuthorizationResponse()
                    html_content = await commitments_resp.content.read()
                    url_str, parameters = form_parser(html_content, username, password)
                    if url_str is None:
                        url = commitments_resp.url
                    else:
                        url = commitments_resp.url.join(URL(url_str))

                    oauth2_resp = await session.post(url, data=parameters)

                    if oauth2_resp.status != 200:
                        raise InvalidAuthorizationResponse()

                    commitments_resp = await session.post(self.token_url.join(URL('commitments')))

                    if commitments_resp.headers.get("Content-Type") != 'application/x-msgpack':
                        raise InvalidAuthorizationResponse()

                commitments: List[SignerCommitMessage] = unpackb(await commitments_resp.content.read())

                challenges, challenges_internal, token_secret_keys = generate_challenges(server_key, commitments)

                pretoken_resp = await session.post(
                    self.token_url.join(URL('pretokens')),
                    headers={'Content-Type': 'application/x-msgpack'},
                    data=packb(challenges)
                )
                pretokens: List[SignerResponseMessage] = unpackb(await pretoken_resp.content.read())
                tokens = generate_tokens(server_key, challenges_internal, token_secret_keys, pretokens)

                # bulk insert tokens in DB
                await self.repository.save_token_server_key(server_key)
                await self.repository.save_tokens(tokens)

                return len(tokens)
        return 0

    async def send_publication(self):
        key_pair = gen_key_pair()
        n_hits, generator = await self.index.publish()
        documents = await self.index.get_documents()
        secret, publication = MSPSIDocumentOwner.publish(generator, documents, n_hits)

        nym = await self.get_or_create_nym()

        payload = PublicationMessage(nym, key_pair.public, publication, len(documents)).to_bytes()
        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL('/bb/broadcast')), data=payload) as response:
                response.raise_for_status()

        await self.repository.save_publication(Publication(key_pair.secret, secret, nym, len(documents)))

    async def get_or_create_nym(self):
        nym = await self.repository.get_parameter("nym")
        if nym is None:
            nym = str(uuid.uuid4())
            await self.repository.set_parameter("nym", nym)
        return nym


def main():
    # for testing
    keys = gen_key_pair()
    dburl = 'sqlite:///dsnet.db'
    engine = create_engine(dburl)
    metadata.create_all(engine)
    repository = SqlalchemyRepository(databases.Database(dburl))
    url = URL('http://localhost:8001')
    token_url = URL('http://localhost:8000')
    api = DsnetApi(
        url,
        token_url,
        repository,
        keys.secret,
        index=MemoryIndex([
            NamedEntity("doc_id", NamedEntityCategory.PERSON, "foo"),
            NamedEntity("doc_id", NamedEntityCategory.PERSON, "bar"),
        ], documents=[Document("doc_id", datetime.datetime.utcnow(), "content with foo bar")]),
        query_type=QueryType.CLEARTEXT,
        message_retriever=AddressMatchMessageRetriever(URL(dburl), repository),
        message_sender=DirectMessageSender(URL(dburl)),
    )

    asyncio.get_event_loop().run_until_complete(api.start_listening())


if __name__ == '__main__':
    main()
