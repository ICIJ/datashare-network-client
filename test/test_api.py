import pytest
from yarl import URL

from dsnetclient.api import DsnetApi
from test.server import UvicornTestServer

server = None


@pytest.fixture
async def startup_and_shutdown_server():
    global server

    """Start server as test fixture and tear down after test"""
    server = UvicornTestServer('dsnetserver.main:app')
    await server.up()
    yield
    await server.down()


@pytest.mark.asyncio
async def test_root(startup_and_shutdown_server):
    assert (await DsnetApi(URL('http://localhost:12345')).get_server_version()) == {
        'message': 'Datashare Network Server version 0.1.0'}
