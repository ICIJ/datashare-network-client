import pytest
import pytest_asyncio

import httpx

from authlib.integrations.httpx_client import AsyncOAuth2Client

from test.server import UvicornTestServer
from test.server_oauth2 import app


@pytest_asyncio.fixture
async def startup_and_shutdown_oauth2_server():
    server = UvicornTestServer(app, port=12345)
    await server.up()
    yield
    await server.down()


@pytest.mark.asyncio
async def test_oauth2_authorize(startup_and_shutdown_oauth2_server):
    client_id = 'foo'
    client_secret = 'bar'
    client = AsyncOAuth2Client(client_id, client_secret, redirect_uri="http://localhost:4000/callback")
    uri, state = client.create_authorization_url('http://127.0.0.1:12345/authorize')

    async with httpx.AsyncClient() as client:
        response = await client.get(uri)
        assert response.status_code == 302
        assert "signin" in response.headers["Location"]
