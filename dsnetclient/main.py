import asyncio

import databases
from dsnet.crypto import gen_key_pair
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.repository import SqlalchemyRepository

DB_URL = 'sqlite:///dsnet.db'


def main():
    my_keys = gen_key_pair()
    repository = SqlalchemyRepository(databases.Database(DB_URL))
    api = DsnetApi(URL('http://localhost:8000'), repository, my_keys.private)

    asyncio.get_event_loop().run_until_complete(api.start_listening())


if __name__ == '__main__':
    main()