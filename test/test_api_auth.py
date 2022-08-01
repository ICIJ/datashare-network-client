import os

import databases
import pytest
import pytest_asyncio
from dsnet.token import AbeToken
from sqlalchemy import create_engine
from sscred.blind_signature import AbeParam, AbePublicKey
from sscred.pack import packb
from yarl import URL

from dsnetclient.api import DsnetApi, InvalidAuthorizationResponse
from dsnetclient.form_parser import bs_parser
from dsnetclient.message_retriever import AddressMatchMessageRetriever
from dsnetclient.message_sender import DirectMessageSender
from dsnetclient.models import metadata as metadata_client
from dsnetclient.repository import SqlalchemyRepository
from test.server import UvicornTestServer

DATABASE_URL = 'sqlite:///auth_test.db'
database = databases.Database(DATABASE_URL)
pkey = None
TOKEN_SERVER_PORT = 12345
os.environ['TOKEN_SERVER_PORT'] = str(TOKEN_SERVER_PORT)
os.environ['TOKEN_SERVER_NBTOKENS'] = str(3)


@pytest.fixture
def pkey():
    global pkey
    params = AbeParam()
    skey, pkey = params.generate_new_key_pair()
    os.environ['TOKEN_SERVER_SKEY'] = packb(skey).hex()
    return pkey


@pytest_asyncio.fixture
async def connect_disconnect_db():
    engine = create_engine(DATABASE_URL)
    metadata_client.create_all(engine)
    await database.connect()
    yield
    metadata_client.drop_all(engine)
    await database.disconnect()


@pytest_asyncio.fixture
async def startup_and_shutdown_servers(connect_disconnect_db, pkey):
    id_server = UvicornTestServer('test.server_oauth2:app', port=12346)
    token_server = UvicornTestServer('tokenserver.main:app', port=TOKEN_SERVER_PORT)
    await id_server.up()
    await token_server.up()
    yield
    await id_server.down()
    await token_server.down()


@pytest.mark.asyncio
async def test_fetch_token_not_authenticated(startup_and_shutdown_servers):
    repository = SqlalchemyRepository(database)
    api = DsnetApi(
        URL('http://notused'),
        URL(f'http://localhost:{TOKEN_SERVER_PORT}'),
        repository,
        message_retriever=AddressMatchMessageRetriever(URL('http://notused'), repository),
        message_sender=DirectMessageSender(URL('http://notused')),
        secret_key=b"dummy",
    )
    with pytest.raises(InvalidAuthorizationResponse):
        await api.fetch_pre_tokens(None, None, None)


@pytest.mark.asyncio
async def test_fetch_token_with_authentication_bad_login(startup_and_shutdown_servers):
    repository = SqlalchemyRepository(database)
    api = DsnetApi(
        URL('http://notused'),
        URL(f'http://localhost:{TOKEN_SERVER_PORT}'),
        repository,
        message_retriever=AddressMatchMessageRetriever(URL('http://notused'), repository),
        message_sender=DirectMessageSender(URL('http://notused')),
        secret_key=b"dummy",
    )
    with pytest.raises(InvalidAuthorizationResponse):
        await api.fetch_pre_tokens('user', 'bad_password', bs_parser)


@pytest.mark.asyncio
@pytest.mark.skip
async def test_auth_epoch_tokens_already_downloaded(startup_and_shutdown_servers, pkey):
    repository = SqlalchemyRepository(database)
    await repository.save_token_server_key(pkey)
    api = DsnetApi(
        URL('http://notused'),
        URL(f'http://localhost:{TOKEN_SERVER_PORT}'),
        repository,
        message_retriever=AddressMatchMessageRetriever(URL('http://notused'), repository),
        message_sender=DirectMessageSender(URL('http://notused')),
        secret_key=b"dummy",
    )
    assert 0 == await api.fetch_pre_tokens('johndoe', 'secret', bs_parser)


@pytest.mark.asyncio
@pytest.mark.skip
async def test_auth_get_tokens_with_form_parser_url_none(pkey, startup_and_shutdown_servers):
    repository = SqlalchemyRepository(database)
    url = URL('http://notused')
    api = DsnetApi(
        url,
        URL(f'http://localhost:{TOKEN_SERVER_PORT}'),
        repository,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        secret_key=b"dummy"
    )
    assert 3 == await api.fetch_pre_tokens('johndoe', 'secret', lambda html, u, p: (None, {'username': 'johndoe', 'password': 'secret'}))


@pytest.mark.asyncio
@pytest.mark.skip
async def test_auth_get_tokens_with_form_parser_url_relative(pkey, startup_and_shutdown_servers):
    repository = SqlalchemyRepository(database)
    url = URL('http://notused')
    api = DsnetApi(
        url,
        URL(f'http://localhost:{TOKEN_SERVER_PORT}'),
        repository,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        secret_key=b"dummy"
    )
    assert 3 == await api.fetch_pre_tokens('johndoe', 'secret', lambda html, u, p: ('/signin', {'username': 'johndoe', 'password': 'secret'}))


@pytest.mark.asyncio
async def test_auth_get_tokens(pkey, startup_and_shutdown_servers):
    repository = SqlalchemyRepository(database)
    url = URL('http://notused')
    api = DsnetApi(
        url,
        URL(f'http://localhost:{TOKEN_SERVER_PORT}'),
        repository,
        message_retriever=AddressMatchMessageRetriever(url, repository),
        message_sender=DirectMessageSender(url),
        secret_key=b"dummy"
    )
    assert 3 == await api.fetch_pre_tokens('johndoe', 'secret', bs_parser)

    server_key: AbePublicKey = await repository.get_token_server_key()
    assert server_key is not None
    assert isinstance(server_key, AbePublicKey)

    token: AbeToken = await repository.pop_token()
    assert isinstance(token, AbeToken)
    assert server_key.verify_signature(token.token)

    assert (await repository.pop_token()) is not None
    assert (await repository.pop_token()) is not None
    assert (await repository.pop_token()) is None
