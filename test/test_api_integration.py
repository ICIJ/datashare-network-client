import asyncio
import datetime
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


DATABASE_URL = 'sqlite:///dsnet.db'

async def dummy_cb(_) -> None: pass


@pytest_asyncio.fixture
async def connect_disconnect_db():
    database = databases.Database(DATABASE_URL)
    engine = create_engine(DATABASE_URL)
    metadata_client.create_all(engine)
    metadata_server.create_all(engine)
    await database.connect()
    yield database
    metadata_client.drop_all(engine)
    metadata_server.drop_all(engine)
    await database.disconnect()


@pytest_asyncio.fixture
async def startup_and_shutdown_server():
    server = UvicornTestServer('dsnetserver.main:app', port=12345)
    await server.up()
    yield
    await server.down()


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
            'server_version': dsnetserver.__version__}


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_send_query(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(connect_disconnect_db)
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
async def test_send_publication(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(connect_disconnect_db)
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
async def test_close_api(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(connect_disconnect_db)
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
async def test_websocket_reconnect(connect_disconnect_db):
    repository = SqlalchemyRepository(connect_disconnect_db)
    tokens, pk = create_tokens(1)
    await repository.save_tokens(tokens)
    await repository.save_token_server_key(pk)
    local_server = UvicornTestServer('dsnetserver.main:app', port=23456)
    await local_server.up()

    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    cb_called = Event()

    async def cb(payload: Message) -> None:
        assert payload is not None
        cb_called.set()

    url = URL('http://localhost:23456')
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

    await local_server.down()
    await local_server.up()
    await asyncio.sleep(0.2)

    await api.send_query(b'payload_value')
    await api.close()
    await cb_called.wait()
    await local_server.down()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_send_response(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(connect_disconnect_db)
    tokens, pk = create_tokens(1)
    await repository.save_tokens(tokens)
    await repository.save_token_server_key(pk)
    keys_alice = gen_key_pair()
    keys_bob = gen_key_pair()
    await repository.save_peer(Peer(keys_alice.public))
    await repository.save_peer(Peer(keys_bob.public))

    url = URL('http://localhost:12345')

    retriever = ProbabilisticCoverMessageRetriever(url, repository, lambda: False)

    api_alice = DsnetApi(
        url,
        None,
        repository,
        secret_key=keys_alice.secret,
        message_retriever=retriever,
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT
    )
    api_bob = DsnetApi(
        url,
        None,
        repository,
        secret_key=keys_bob.secret,
        message_retriever=retriever,
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT
    )

    async def cb_alice(message: Message):
        if message.type() == MessageType.NOTIFICATION:
            convs = await repository.get_conversations_filter_by(querier=True)
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


# @pytest.mark.asyncio
# @pytest.mark.timeout(5)
# async def test_mspsi_query_response(startup_and_shutdown_server, connect_disconnect_db):
#     repository = SqlalchemyRepository(database)
#     tokens, pk = create_tokens(1)
#     await repository.save_tokens(tokens)
#     await repository.save_token_server_key(pk)
#     keys_alice = gen_key_pair()
#     keys_bob = gen_key_pair()
#     await repository.save_peer(Peer(keys_alice.public))
#     await repository.save_peer(Peer(keys_bob.public))
#
#     url = URL('http://localhost:12345')
#
#     api_alice = DsnetApi(
#         url,
#         None,
#         repository,
#         secret_key=keys_alice.secret,
#         message_retriever=AddressMatchMessageRetriever(url, repository),
#         message_sender=DirectMessageSender(url),
#         query_type=QueryType.DPSI,
#         index=MspsiIndex(repository, AsyncMock())
#     )
#     api_bob = DsnetApi(
#         url,
#         None,
#         repository,
#         secret_key=keys_bob.secret,
#         message_retriever=AddressMatchMessageRetriever(url, repository),
#         message_sender=DirectMessageSender(url),
#         query_type=QueryType.DPSI,
#         index=MspsiIndex(repository, AsyncMock())
#     )
#
#     skey, cuckoo_filter = MSPSIDocumentOwner.publish((NamedEntity('doc_id', NamedEntityCategory.PERSON, 'foo'),),
#                                                      [Document("doc_id", datetime.datetime.utcnow())], 1)
#     await repository.save_publication_message(PublicationMessage("nym_bob", keys_bob.public, cuckoo_filter, 1))
#     await repository.save_publication(Publication(keys_bob.secret, skey, 'nym_bob', 1))
#
#     async def cb_alice(message: Message):
#         await api_alice.websocket_callback(message)
#     async def cb_bob(message: Message):
#         await api_bob.websocket_callback(message)
#
#     alice_task = api_alice.background_listening(cb_alice)
#     bob_task = api_bob.background_listening(cb_bob)
#
#     await api_alice.send_query(b"foo")
#
#     await api_bob.close()
#     await api_alice.close()
#
#     await alice_task
#     await bob_task
#
#     assert len(await repository.get_conversations()) == 3
#     alice_conversations = await repository.get_conversations_filter_by(querier=True)
#     assert alice_conversations[0].nb_recv_messages == 1
#     assert alice_conversations[0].last_message.type() == MessageType.RESPONSE
