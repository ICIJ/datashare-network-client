import pytest

from test.server import UvicornTestServer


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_server():
    local_server = UvicornTestServer('dsnetserver.main:app', port=23456)
    await local_server.up()
    await local_server.down()
    await local_server.up()
    await local_server.down()
