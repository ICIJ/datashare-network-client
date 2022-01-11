import databases
import pytest
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.repository import SqlalchemyRepository
from test.server import UvicornTestServer

DATABASE_URL = 'sqlite:///dsnet.db'
database = databases.Database(DATABASE_URL)

@pytest.fixture
async def startup_and_shutdown_server():
    server = UvicornTestServer('dsnetserver.main:app')
    await server.up()
    yield
    await server.down()


@pytest.mark.asyncio
async def test_root(startup_and_shutdown_server):
    assert await DsnetApi(URL('http://localhost:12345')).get_server_version() == {
        'message': 'Datashare Network Server version 0.1.0'}


@pytest.mark.skip(reason='TODO integration test')
@pytest.mark.asyncio
async def test_send_query(startup_and_shutdown_server):
    repository = SqlalchemyRepository(database)
    assert await DsnetApi(URL('http://localhost:12345'), repository).send_query('payload_value') == True