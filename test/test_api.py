from multiprocessing import Process
from time import sleep

import pytest
import uvicorn
from yarl import URL

from dsnetclient.api import DsnetApi
server = None


def setup_module(_):
    global server

    server = Process(target=uvicorn.run,
                     args=('dsnetserver.main:app',),
                     kwargs={
                         "host": "127.0.0.1",
                         "port": 12345,
                         "log_level": "info"},
                     daemon=True)
    server.start()
    sleep(1)


def teardown_module(_):
    global server
    server.terminate()


@pytest.mark.asyncio
async def test_root():
    assert await DsnetApi(URL('http://localhost:12345')).get_server_version() == {'message': 'Datashare Network Server version 0.1.0'}


