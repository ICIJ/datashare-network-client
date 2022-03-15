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
        assert "/signin" in response.headers["Location"]


@pytest.mark.asyncio
async def test_signin(startup_and_shutdown_oauth2_server):
    async with httpx.AsyncClient() as client:
        oauth_client = AsyncOAuth2Client('foo', 'bar', redirect_uri="http://localhost:12345/callback")
        authorization_url, state = oauth_client.create_authorization_url('http://localhost:12345/authorize')

        # user is authenticating
        response = await client.get(authorization_url, follow_redirects=True)
        assert response.status_code == 200
        assert 'text/html' in response.headers.get('content-type')
        resp_post = await client.post(f"http://localhost:12345/signin?state={state}", data={
            'username': 'johndoe',
            "password": 'secret',
        })
        assert resp_post.status_code == 302
        # end auth

        response_token = await oauth_client.fetch_token("http://127.0.0.1:12345/token", authorization_response=resp_post.headers['location'])
        assert response_token is not None

        # Fetch authenticated resource
        is_me = await oauth_client.get('http://127.0.0.1:12345/users/me')
        assert is_me.status_code == 200
        assert is_me.json()['username'] == 'johndoe'

