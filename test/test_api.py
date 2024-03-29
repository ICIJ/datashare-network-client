import datetime
import re

import databases
import pytest
import pytest_asyncio
from cuckoo.filter import BCuckooFilter
from dsnet.core import QueryType
from dsnet.crypto import gen_key_pair
from dsnet.message import Query, PigeonHoleNotification, PigeonHoleMessage, PublicationMessage
from pytest_httpserver import HTTPServer
from pytest_httpserver.httpserver import HandlerType
from sqlalchemy import create_engine
from werkzeug import Response
from yarl import URL

from dsnetclient.api import DsnetApi, NoTokenException, InvalidAuthorizationResponse
from dsnetclient.index import MemoryIndex, NamedEntity, NamedEntityCategory, Document
from dsnetclient.message_retriever import AddressMatchMessageRetriever
from dsnetclient.message_sender import DirectMessageSender
from dsnetclient.models import metadata
from dsnetclient.repository import SqlalchemyRepository, Peer
from test.test_utils import create_tokens

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


@pytest.fixture
def memory_index() -> MemoryIndex:
    return MemoryIndex([
        NamedEntity("doc_id", NamedEntityCategory.PERSON, "foo"),
        NamedEntity("doc_id", NamedEntityCategory.PERSON, "bar"),
    ], [
        Document("doc_id", datetime.datetime.utcnow(), "content for doc_id")
    ])


@pytest.mark.asyncio
async def test_send_query_no_tokens(httpserver: HTTPServer, connect_disconnect_db):
    api = await create_api(httpserver, number_tokens=0)

    with pytest.raises(NoTokenException):
        await api.send_query(b'raw query')


@pytest.mark.asyncio
async def test_send_query(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request("/bb/broadcast", method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver)

    await api.send_query(b'raw query')
    httpserver.check()

    conversations = await api.repository.get_conversations()
    assert len(conversations) == 2
    assert conversations[0].query == b'raw query'


@pytest.mark.asyncio
async def test_send_publication(httpserver: HTTPServer, memory_index: MemoryIndex, connect_disconnect_db):
    httpserver.expect_request("/bb/broadcast", method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver, memory_index)

    await api.send_publication()
    httpserver.check()

    publications = await api.repository.get_publications()
    assert len(publications) == 1
    assert publications[0].nb_docs == 1
    assert publications[0].nym == await api.repository.get_parameter("nym")


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
async def test_receive_query_matches(httpserver: HTTPServer, memory_index: MemoryIndex, connect_disconnect_db):
    httpserver.expect_request(re.compile(r"/ph/.+"), method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver, memory_index)
    token = await api.repository.pop_token()

    await api.handle_query(Query.create(gen_key_pair().public, token,  b'foo'))

    httpserver.check()
    conversations = await api.repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].nb_sent_messages == 1


@pytest.mark.asyncio
async def test_receive_query_does_not_match(httpserver: HTTPServer, connect_disconnect_db):
    httpserver.expect_request(re.compile(r"/ph/.+"), method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver, MemoryIndex([], []))
    token = await api.repository.pop_token()

    await api.handle_query(Query.create(gen_key_pair().public, token,  b'foo'))

    httpserver.check()
    conv = (await api.repository.get_conversations())[0]
    assert conv.last_message.payload == b"[]"


@pytest.mark.asyncio
async def test_receive_query_wrong_signature(httpserver: HTTPServer, memory_index: MemoryIndex, connect_disconnect_db):
    api = await create_api(httpserver, memory_index)
    token = await api.repository.pop_token()
    query = Query.create(gen_key_pair().public, token,  b'foo')
    query.signature = b"Wrong signature"

    await api.handle_query(query)

    httpserver.check()
    conversations = await api.repository.get_conversations()
    assert len(conversations) == 0


@pytest.mark.asyncio
async def test_do_treat_my_own_query(httpserver: HTTPServer, memory_index: MemoryIndex, connect_disconnect_db):
    httpserver.expect_request("/bb/broadcast", method='POST', handler_type=HandlerType.ORDERED).respond_with_response(Response(status=200))
    api = await create_api(httpserver, memory_index)

    await api.send_query(b'foo')

    httpserver.check()
    conversations = await api.repository.get_conversations()
    assert len(conversations) == 2
    assert conversations[0].nb_sent_messages == 1
    assert conversations[1].nb_sent_messages == 1


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
    assert len(conversations) == 2
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
    assert len(conversations) == 2
    assert conversations[0].nb_sent_messages == 2
    assert conversations[0].nb_recv_messages == 0


@pytest.mark.asyncio
async def test_get_broadcast_recovery_timestamp(httpserver: HTTPServer, connect_disconnect_db):
    api = await create_api(httpserver)
    ts = await api.broadcast_recovery_timestamp()
    assert isinstance(ts, datetime.datetime)
    assert ts < datetime.datetime.utcnow()


@pytest.mark.asyncio
async def test_get_broadcast_recovery_timestamp_from_last_message(httpserver: HTTPServer, connect_disconnect_db):
    api = await create_api(httpserver)
    await api.repository.save_publication_message(PublicationMessage('nym', b'public_key', BCuckooFilter(1, 0.1), 1))
    expected_ts = await api.repository.get_last_broadcast_timestamp()

    actual_ts = await api.broadcast_recovery_timestamp()

    assert expected_ts == actual_ts


async def create_api(httpserver, index=None, number_tokens=3):
    my_keys = gen_key_pair()
    other = gen_key_pair()
    repository = SqlalchemyRepository(database)
    await repository.save_peer(Peer(other.public))
    await repository.save_peer(Peer(my_keys.public))
    url = URL(httpserver.url_for('/'))
    api = DsnetApi(
        url,
        None,
        repository,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        query_type=QueryType.CLEARTEXT,
        secret_key=my_keys.secret,
        index=index
    )
    if number_tokens:
        tokens, server_key = create_tokens(number_tokens)
        await repository.save_tokens(tokens)
        await repository.save_token_server_key(server_key)
    return api
