import os

import databases
import httpx
import pytest
import pytest_asyncio
from authlib.integrations.httpx_client import AsyncOAuth2Client
from dsnet.token import AbeToken
from sqlalchemy import create_engine
from sscred.blind_signature import AbeParam, AbePublicKey
from sscred.pack import packb, unpackb
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.message_sender import DirectMessageSender
from dsnetclient.models import metadata as metadata_client
from dsnetclient.repository import SqlalchemyRepository
from test.server import UvicornTestServer

DATABASE_URL = 'sqlite:///auth_test.db'
database = databases.Database(DATABASE_URL)
pkey = None
TOKEN_SERVER_PORT = 12346
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
    id_server = UvicornTestServer('test.server_oauth2:app', port=12345)
    token_server = UvicornTestServer('tokenserver.main:app', port=TOKEN_SERVER_PORT)
    await id_server.up()
    await token_server.up()
    yield
    await id_server.down()
    await token_server.down()


@pytest.mark.asyncio
async def test_auth_epoch_tokens_already_downloaded(startup_and_shutdown_servers):
    repository = SqlalchemyRepository(database)
    await repository.save_token_server_key(pkey)
    url = URL('http://notused')
    api = DsnetApi(
        url,
        repository,
        message_sender=DirectMessageSender(url),
        secret_key=b"dummy",
        oauth_client=AsyncOAuth2Client('foo', 'bar', redirect_uri="http://localhost:12345/callback", base_url=f"http://localhost:12345")
    )
    url, _ = api.start_auth('http://localhost:12345/authorize')
    assert url is not None    # will be displayed to user for use in browser
    returned_url = await authenticate(url, 'johndoe', 'secret')  # will be pasted by the user in ds client CLI
    await api.end_auth("http://localhost:12345/token", returned_url)

    assert 0 == (await api.fetch_pre_tokens())


@pytest.mark.asyncio
async def test_auth_get_tokens(pkey, startup_and_shutdown_servers):
    repository = SqlalchemyRepository(database)
    url = URL('http://notused')
    api = DsnetApi(
        url,
        repository,
        message_sender=DirectMessageSender(url),
        secret_key=b"dummy",
        oauth_client=AsyncOAuth2Client('foo', 'bar', redirect_uri="http://localhost:12345/callback", base_url=f"http://localhost:12345")
    )
    url, _ = api.start_auth('http://localhost:12345/authorize')
    returned_url = await authenticate(url, 'johndoe', 'secret')  # will be pasted by the user in ds client CLI
    await api.end_auth("http://localhost:12345/token", returned_url)

    assert 3 == await api.fetch_pre_tokens()

    server_key: AbePublicKey = await repository.get_token_server_key()
    assert server_key is not None
    assert isinstance(server_key, AbePublicKey)

    token: AbeToken = await repository.pop_token()
    assert isinstance(token, AbeToken)
    assert server_key.verify_signature(token.token)

    assert (await repository.pop_token()) is not None
    assert (await repository.pop_token()) is not None
    assert (await repository.pop_token()) is None


async def authenticate(authorize_url, username, password) -> str:
    async with httpx.AsyncClient() as client:
        await client.get(authorize_url)
        response = await client.post("http://localhost:12345/signin", data={
            'username': username,
            "password": password
        })
        return response.headers['location']
