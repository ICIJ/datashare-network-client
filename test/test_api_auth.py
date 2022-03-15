import databases
import httpx
import pytest
import pytest_asyncio
from authlib.integrations.httpx_client import AsyncOAuth2Client
from sqlalchemy import create_engine
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.models import metadata as metadata_client
from test.server import UvicornTestServer

DATABASE_URL = 'sqlite:///auth_test.db'
database = databases.Database(DATABASE_URL)


@pytest_asyncio.fixture
async def connect_disconnect_db():
    engine = create_engine(DATABASE_URL)
    metadata_client.create_all(engine)
    await database.connect()
    yield
    metadata_client.drop_all(engine)
    await database.disconnect()


@pytest_asyncio.fixture
async def startup_and_shutdown_server():
    server = UvicornTestServer('test.server_oauth2:app')
    await server.up()
    yield
    await server.down()


@pytest.mark.asyncio
async def test_auth(startup_and_shutdown_server):
    api = DsnetApi(URL('http://notused'), None, secret_key=b"dummy",
                   oauth_client=AsyncOAuth2Client('foo', 'bar', redirect_uri="http://localhost:12345/callback"))
    url, _ = api.start_auth('http://localhost:12345/authorize')
    assert url is not None    # will be displayed to user for use in browser
    returned_url = await authenticate(url, 'johndoe', 'secret')  # will be pasted by the user in ds client CLI
    await api.end_auth("http://localhost:12345/token", returned_url)

    assert 1 == await api.fetch_pre_tokens(nb_tokens=3)


async def authenticate(authorize_url, username, password) -> str:
    async with httpx.AsyncClient() as client:
        await client.get(authorize_url)
        response = await client.post("http://localhost:12345/signin", data={
            'username': username,
            "password": password
        })
        return response.headers['location']
