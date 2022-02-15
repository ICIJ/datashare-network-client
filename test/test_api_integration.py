import asyncio
from asyncio import Event

import databases
import dsnetserver
import pytest
import pytest_asyncio
from dsnet.message import MessageType, Message
from dsnet.crypto import gen_key_pair
from dsnetserver.models import metadata as metadata_server
from sqlalchemy import create_engine
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.models import metadata as metadata_client
from dsnetclient.repository import SqlalchemyRepository, Peer
from test.server import UvicornTestServer

DATABASE_URL = 'sqlite:///dsnet.db'
database = databases.Database(DATABASE_URL)
async def dummy_cb(_) -> None: pass


@pytest_asyncio.fixture
async def connect_disconnect_db():
    engine = create_engine(DATABASE_URL)
    metadata_client.create_all(engine)
    metadata_server.create_all(engine)
    await database.connect()
    yield
    metadata_client.drop_all(engine)
    metadata_server.drop_all(engine)
    await database.disconnect()


@pytest_asyncio.fixture
async def startup_and_shutdown_server():
    server = UvicornTestServer('dsnetserver.main:app')
    await server.up()
    yield
    await server.down()


@pytest.mark.asyncio
async def test_root(startup_and_shutdown_server):
    assert await DsnetApi(URL('http://localhost:12345'), None, secret_key=b"dummy").get_server_version() == {
        'message': f'Datashare Network Server version {dsnetserver.__version__}'}


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_send_query(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    cb_called = Event()

    async def cb(message: Message) -> None:
        assert message is not None
        assert message.type() == MessageType.QUERY
        assert message.payload == b'payload_value'
        cb_called.set()

    api = DsnetApi(URL('http://localhost:12345'), repository, secret_key=keys.secret)
    api.background_listening(cb)
    await api.send_query(b'payload_value')

    await cb_called.wait()
    await api.close()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_close_api(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))
    api = DsnetApi(URL('http://localhost:12345'), repository, secret_key=keys.secret)
    task = api.background_listening(dummy_cb)
    await api.close()
    await task
    assert task.done()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_websocket_reconnect(connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    local_server = UvicornTestServer('dsnetserver.main:app', port=23456)
    await local_server.up()

    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    cb_called = Event()

    async def cb(payload: Message) -> None:
        assert payload is not None
        cb_called.set()

    api = DsnetApi(URL('http://localhost:23456'), repository, secret_key=keys.secret, reconnect_delay_seconds=0.1)
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
    repository = SqlalchemyRepository(database)
    keys_alice = gen_key_pair()
    keys_bob = gen_key_pair()
    await repository.save_peer(Peer(keys_alice.public))
    await repository.save_peer(Peer(keys_bob.public))

    api_alice = DsnetApi(URL('http://localhost:12345'), repository, secret_key=keys_alice.secret)
    api_bob = DsnetApi(URL('http://localhost:12345'), repository, secret_key=keys_bob.secret)

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
