import asyncio
from asyncio import Event

import databases
import pytest
import pytest_asyncio
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
    assert await DsnetApi(URL('http://localhost:12345'), None).get_server_version() == {
        'message': 'Datashare Network Server version 0.2.1'}


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_send_query(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    cb_called = Event()

    async def cb(payload: bytes) -> None:
        assert payload is not None
        assert payload[0] == 1
        assert payload[-len(b'payload_value'):] == b'payload_value'
        cb_called.set()

    api = DsnetApi(URL('http://localhost:12345'), repository, cb)
    await api.send_query('payload_value')

    conversations = await repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].query == 'payload_value'

    await cb_called.wait()
    await api.close()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_close_api(startup_and_shutdown_server, connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))
    api = DsnetApi(URL('http://localhost:12345'), repository, dummy_cb)
    await api.close()
    assert api._listener.done()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_close_websocket(connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    local_server = UvicornTestServer('dsnetserver.main:app', port=23456)
    await local_server.up()

    keys = gen_key_pair()
    await repository.save_peer(Peer(keys.public))

    cb_called = Event()

    async def cb(payload: bytes) -> None:
        assert payload is not None
        cb_called.set()

    api = DsnetApi(URL('http://localhost:23456'), repository, cb, reconnect_delay_seconds=0.1)
    await local_server.down()
    await local_server.up()
    await asyncio.sleep(0.2)

    await api.send_query('payload_value')
    await api.close()
    await cb_called.wait()
    await local_server.down()
