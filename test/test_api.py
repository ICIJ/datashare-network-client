import re
from unittest.mock import Mock

import databases
import pytest
import pytest_asyncio
from dsnet.crypto import gen_key_pair
from dsnet.message import Query, PigeonHoleNotification, PigeonHoleMessage
from pytest_httpserver import HTTPServer
from pytest_httpserver.httpserver import HandlerType
from sqlalchemy import create_engine
from werkzeug import Response
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.index import MemoryIndex, Index
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
    api = await create_api(httpserver)

    await api.send_query(b'raw query')
    httpserver.check()

    conversations = await api.repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].query == b'raw query'


@pytest.mark.asyncio
async def test_send_response(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request(re.compile(r"/ph/.+"), method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver)

    await api.send_response(gen_key_pair().public, b'response payload')
    httpserver.check()

    conversations = await api.repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].nb_sent_messages == 1


@pytest.mark.asyncio
async def test_receive_query_matches(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request(re.compile(r"/ph/.+"), method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver, MemoryIndex({'foo', 'bar'}))

    await api.handle_query(Query(gen_key_pair().public, b'foo'))

    httpserver.check()
    conversations = await api.repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].nb_sent_messages == 1


@pytest.mark.asyncio
async def test_receive_query_does_not_match(httpserver: HTTPServer, connect_disconnect_db):
    api = await create_api(httpserver, MemoryIndex(set()))

    await api.handle_query(Query(gen_key_pair().public, b'foo'))

    httpserver.check()
    assert await api.repository.get_conversations() == []


@pytest.mark.asyncio
async def test_do_not_treat_my_own_query(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request("/bb/broadcast", method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    mocked_index = Mock(Index)
    api = await create_api(httpserver, mocked_index)

    await api.send_query(b'foo')
    conv = (await api.repository.get_conversations())[0]
    await api.handle_query(Query(conv.public_key, b'foo'))

    mocked_index.search.assert_not_called()


@pytest.mark.asyncio
async def test_receive_ph_notification_no_listening_address(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.respond_permanent_failure()
    api = await create_api(httpserver)
    await api.handle_ph_notification(PigeonHoleNotification('beef')) # if server is called it will break


@pytest.mark.asyncio
async def test_receive_ph_notification_with_matching_address_as_querier(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request("/bb/broadcast", method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver)
    await api.send_query(b'query')
    conv = (await api.repository.get_conversations())[0]
    msg = PigeonHoleMessage(conv.last_address, conv.pigeonhole_for_address(conv.last_address).encrypt(b'response'), gen_key_pair().public)
    httpserver.expect_request(re.compile(f"/ph/{conv.last_address.hex()}"), method='GET', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200, response=msg.to_bytes(), content_type='application/octet-stream'))

    await api.handle_ph_notification(PigeonHoleNotification.from_address(conv.last_address))

    httpserver.check()
    conversations = await api.repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].nb_sent_messages == 1
    assert conversations[0].nb_recv_messages == 1


@pytest.mark.asyncio
async def test_receive_ph_notification_with_matching_address_as_recipient(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request(re.compile(r"/ph/.+"), method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver)
    query_keys = gen_key_pair()
    await api.send_response(query_keys.public, b'response')
    conv = (await api.repository.get_conversations())[0]
    msg = PigeonHoleMessage(conv.last_address, conv.pigeonhole_for_address(conv.last_address).encrypt(b'response'), gen_key_pair().public)
    httpserver.expect_request(re.compile(f"/ph/{conv.last_address.hex()}"), method='GET', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200, response=msg.to_bytes(), content_type='application/octet-stream'))

    await api.handle_ph_notification(PigeonHoleNotification.from_address(conv.last_address))

    httpserver.check()
    conversations = await api.repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].nb_sent_messages == 1
    assert conversations[0].nb_recv_messages == 2


@pytest.mark.asyncio
async def test_send_message(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request("/bb/broadcast", method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    httpserver.expect_request(re.compile(r"/ph/.+"), method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver)
    await api.send_query(b'initial query')

    await api.send_message(1, b'hello bob')

    httpserver.check()
    conversations = await api.repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].nb_sent_messages == 2
    assert conversations[0].nb_recv_messages == 0


async def create_api(httpserver, index = None):
    my_keys = gen_key_pair()
    other = gen_key_pair()
    repository = SqlalchemyRepository(database)
    await repository.save_peer(Peer(other.public))
    api = DsnetApi(URL(httpserver.url_for('/')), repository, secret_key=my_keys.secret, index=index)
    return api