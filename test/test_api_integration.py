import asyncio
import datetime
import os
from asyncio import Event
from unittest.mock import AsyncMock

import databases
import dsnet
import dsnetserver
import pytest
import pytest_asyncio
from dsnet.core import QueryType
from dsnet.crypto import gen_key_pair
from dsnet.message import MessageType, Message, PublicationMessage
from dsnet.mspsi import NamedEntity, NamedEntityCategory, Document, MSPSIDocumentOwner
from dsnetserver.models import metadata as metadata_server
from sqlalchemy import create_engine
from sscred import unpackb
from tokenserver.test.server import UvicornTestServer
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.index import Index, MspsiIndex
from dsnetclient.message_retriever import AddressMatchMessageRetriever, ProbabilisticCoverMessageRetriever
from dsnetclient.message_sender import DirectMessageSender
from dsnetclient.models import metadata as metadata_client
from dsnetclient.repository import SqlalchemyRepository, Peer, Publication
from test.test_utils import create_tokens

DATABASE_URL_SERVER = 'sqlite:///dsnet_server.db'
DATABASE_URL_ALICE = 'sqlite:///dsnet_alice.db'
DATABASE_URL_BOB = 'sqlite:///dsnet_bob.db'


async def dummy_cb(_) -> None: pass


@pytest_asyncio.fixture
async def db_alice():
    engine, database = await init_db(DATABASE_URL_ALICE)
    yield database
    await close_db(engine, database)


@pytest_asyncio.fixture
async def db_bob():
    engine, database = await init_db(DATABASE_URL_BOB)
    yield database
    await close_db(engine, database)


async def init_db(url: str):
    database = databases.Database(url)
    engine = create_engine(url)
    metadata_client.create_all(engine)
    await database.connect()
    return engine, database


async def close_db(engine, database):
    metadata_client.drop_all(engine)
    await database.disconnect()


@pytest_asyncio.fixture
async def startup_and_shutdown_server():
    engine = create_engine(DATABASE_URL_SERVER)
    os.environ['DS_DATABASE_URL'] = DATABASE_URL_SERVER
    metadata_server.create_all(engine)
    server = UvicornTestServer('dsnetserver.main:app', port=12345)
    await server.up()
    yield server
    await server.down()
    metadata_server.drop_all(engine)


@pytest.mark.asyncio
async def test_root(startup_and_shutdown_server):
    url = URL('http://localhost:12345')
    assert await DsnetApi(
            url,
            None,
            None,
            secret_key=b"dummy",
            message_retriever=AddressMatchMessageRetriever(url, None),
            message_sender=DirectMessageSender(url),
            query_type=QueryType.CLEARTEXT
        ).get_server_version() == \
           {'message': f'Datashare Network Server version {dsnetserver.__version__}',
            'core_version': dsnet.__version__,
            'server_version': dsnetserver.__version__,
            'query_type': QueryType.CLEARTEXT}


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_send_query(startup_and_shutdown_server, db_alice):
    repository = SqlalchemyRepository(db_alice)
    tokens, pk = create_tokens(1)
    await repository.save_tokens(tokens)
    await repository.save_token_server_key(pk)
    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    cb_called = Event()

    async def cb(message: Message) -> None:
        assert message is not None
        assert message.type() == MessageType.QUERY
        assert unpackb(message.payload) == [b'payload_value']
        cb_called.set()

    url = URL('http://localhost:12345')
    api = DsnetApi(
        url,
        None,
        repository,
        secret_key=keys.secret,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT
    )
    api.background_listening(cb)
    await api.send_query(b'payload_value')

    await cb_called.wait()
    await api.close()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_send_publication(startup_and_shutdown_server, db_alice):
    repository = SqlalchemyRepository(db_alice)
    tokens, pk = create_tokens(1)
    await repository.save_tokens(tokens)
    await repository.save_token_server_key(pk)
    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    index = AsyncMock(Index, side_effect=[[Document('doc_id', datetime.datetime.utcnow())], ])
    index.get_documents = AsyncMock(return_value=[Document('doc_id', datetime.datetime.utcnow())])
    index.publish = AsyncMock(return_value=(1, (ne for ne in [NamedEntity('doc_id', NamedEntityCategory.PERSON, 'foo')])))

    url = URL('http://localhost:12345')
    api = DsnetApi(
        url,
        None,
        repository,
        secret_key=keys.secret,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT,
        index=index
    )
    task = api.background_listening()
    await api.send_publication()
    await api.close()
    await task
    assert len(await repository.get_publications()) == 1
    assert len(await repository.get_publication_messages()) == 1


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_close_api(startup_and_shutdown_server, db_alice):
    repository = SqlalchemyRepository(db_alice)
    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))
    url = URL('http://localhost:12345')
    api = DsnetApi(
        url,
        None,
        repository,
        secret_key=keys.secret,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT
    )
    task = api.background_listening(dummy_cb)
    await api.close()
    await task
    assert task.done()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_websocket_reconnect(startup_and_shutdown_server, db_alice):
    repository = SqlalchemyRepository(db_alice)
    tokens, pk = create_tokens(1)
    await repository.save_tokens(tokens)
    await repository.save_token_server_key(pk)

    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    cb_called = Event()

    async def cb(payload: Message) -> None:
        assert payload is not None
        cb_called.set()

    url = URL('http://localhost:12345')
    api = DsnetApi(
        url,
        None,
        repository,
        secret_key=keys.secret,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT,
        reconnect_delay_seconds=0.1,
    )
    api.background_listening(cb)

    await startup_and_shutdown_server.down()
    await startup_and_shutdown_server.up()
    await asyncio.sleep(0.2)

    await api.send_query(b'payload_value')
    await api.close()
    await cb_called.wait()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_send_response(startup_and_shutdown_server, db_alice, db_bob):
    repository_alice = SqlalchemyRepository(db_alice)
    repository_bob = SqlalchemyRepository(db_bob)
    tokens, pk = create_tokens(1)
    await repository_alice.save_tokens(tokens)
    await repository_alice.save_token_server_key(pk)
    keys_alice = gen_key_pair()
    keys_bob = gen_key_pair()
    await repository_bob.save_peer(Peer(keys_alice.public))
    await repository_alice.save_peer(Peer(keys_bob.public))

    url = URL('http://localhost:12345')

    retriever = ProbabilisticCoverMessageRetriever(url, repository_bob, lambda: False)

    api_alice = DsnetApi(
        url,
        None,
        repository_alice,
        secret_key=keys_alice.secret,
        message_retriever=retriever,
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT
    )
    api_bob = DsnetApi(
        url,
        None,
        repository_bob,
        secret_key=keys_bob.secret,
        message_retriever=retriever,
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT
    )

    async def cb_alice(message: Message):
        if message.type() == MessageType.NOTIFICATION:
            convs = await repository_alice.get_conversations_filter_by(querier=True)
            assert len(convs) == 1
            assert message.adr_hex == convs[0].last_address[0:3]

    async def cb_bob(message: Message):
        if message.type() == MessageType.QUERY:
            await api_bob.send_response(message.public_key, b"response payload")

    task_alice = api_alice.background_listening(cb_alice)
    task_bob = api_bob.background_listening(cb_bob)

    await api_alice.send_query(b"query payload")

    await api_bob.close()
    await api_alice.close()
    await task_bob
    await task_alice


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_mspsi_query_response(startup_and_shutdown_server, db_alice, db_bob):
    repository_alice = SqlalchemyRepository(db_alice)
    repository_bob = SqlalchemyRepository(db_bob)
    tokens, pk = create_tokens(1)

    await repository_alice.save_tokens(tokens)
    await repository_alice.save_token_server_key(pk)
    await repository_bob.save_token_server_key(pk)

    keys_alice = gen_key_pair()
    keys_bob = gen_key_pair()

    await repository_bob.save_peer(Peer(keys_alice.public))
    await repository_bob.save_peer(Peer(keys_bob.public))

    await repository_alice.save_peer(Peer(keys_bob.public))
    await repository_alice.save_peer(Peer(keys_alice.public))

    url = URL('http://localhost:12345')

    api_alice = DsnetApi(
        url,
        None,
        repository_alice,
        secret_key=keys_alice.secret,
        message_retriever=AddressMatchMessageRetriever(url, repository_alice),
        message_sender=DirectMessageSender(url),
        query_type=QueryType.DPSI,
        index=MspsiIndex(repository_alice, AsyncMock())
    )
    api_bob = DsnetApi(
        url,
        None,
        repository_bob,
        secret_key=keys_bob.secret,
        message_retriever=AddressMatchMessageRetriever(url, repository_bob),
        message_sender=DirectMessageSender(url),
        query_type=QueryType.DPSI,
        index=MspsiIndex(repository_bob, AsyncMock())
    )

    # Bob already published his index
    skey, cuckoo_filter = MSPSIDocumentOwner.publish(
        (NamedEntity('doc_id', NamedEntityCategory.PERSON, 'foo'),),
        [Document("doc_id", datetime.datetime.utcnow())],
        1
    )
    await repository_alice.save_publication_message(PublicationMessage("nym_bob", keys_bob.public, cuckoo_filter, 1))
    await repository_bob.save_publication_message(PublicationMessage("nym_bob", keys_bob.public, cuckoo_filter, 1))
    await repository_bob.save_publication(Publication(secret_key=keys_bob.secret, secret=skey, nym="nym_bob", nb_docs=1))

    alice_event = asyncio.Event()
    alice_expected_messages = 2

    async def cb_alice(message: Message):
        nonlocal alice_event
        nonlocal alice_expected_messages
        alice_expected_messages -= 1
        await api_alice.websocket_callback(message)
        if alice_expected_messages == 0:
            alice_event.set()

    api_alice.background_listening(cb_alice)
    api_bob.background_listening()

    await api_alice.send_query(b"foo")

    await alice_event.wait()

    await api_bob.close()
    await api_alice.close()

    conversations = await repository_alice.get_conversations()
    assert len(conversations) == 2
    alice_with_bob = await repository_alice.get_conversations_filter_by(other_public_key=keys_bob.public)

    assert len(alice_with_bob) == 1
    assert alice_with_bob[0].nb_recv_messages == 1
    assert alice_with_bob[0].last_message.type() == MessageType.RESPONSE
    assert alice_with_bob[0].last_message.payload == b'[[0]]' # first document matches the first keyword (foo)
