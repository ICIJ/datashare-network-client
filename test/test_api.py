import re

import databases
import pytest
import pytest_asyncio
from dsnet.crypto import gen_key_pair
from pytest_httpserver import HTTPServer
from pytest_httpserver.httpserver import HandlerType
from sqlalchemy import create_engine
from werkzeug import Response
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.models import metadata
from dsnetclient.repository import SqlalchemyRepository, Peer

DATABASE_URL = 'sqlite:///dsnet.db'
database = databases.Database(DATABASE_URL)


@pytest_asyncio.fixture
async def connect_disconnect_db():
    engine = create_engine(DATABASE_URL)
    metadata.create_all(engine)
    await database.connect()
    yield
    metadata.drop_all(engine)
    await database.disconnect()


@pytest.mark.asyncio
async def test_send_query(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request("/bb/broadcast", method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    keys = gen_key_pair()
    other = gen_key_pair()
    repository = SqlalchemyRepository(database)
    await repository.save_peer(Peer(other.public))
    api = DsnetApi(URL(httpserver.url_for('/')), repository, private_key=keys.private)

    await api.send_query(b'raw query')
    httpserver.check()

    conversations = await repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].query == b'raw query'


@pytest.mark.asyncio
async def test_send_response(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request(re.compile(r"/ph/.+"), method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))

    keys = gen_key_pair()
    query_keys = gen_key_pair()
    repository = SqlalchemyRepository(database)
    api = DsnetApi(URL(httpserver.url_for('/')), repository, private_key=keys.private)

    await api.send_response(query_keys.public, b'response payload')
    httpserver.check()

    conversations = await repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].nb_sent_messages == 1
