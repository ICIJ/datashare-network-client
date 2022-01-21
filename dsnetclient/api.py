from aiohttp import ClientSession
from aiohttp.abc import HTTPException
from dsnet.core import Conversation, Query
from dsnet.crypto import gen_key_pair

from yarl import URL

from dsnetclient.repository import Repository


class DsnetApi:

    def __init__(self, url: URL, repository: Repository) -> None:
        self.repository = repository
        self.base_url = url
        self.client = ClientSession()

    async def get_server_version(self) -> dict:
        async with self.client.get(self.base_url) as resp:
            return await resp.json()

    async def send_query(self, query: str) -> None:
        query_keys = gen_key_pair()

        for peer in await self.repository.peers():
            conv = Conversation(query_keys.private, peer.public_key, query, querier=True)
            await self.repository.save_conversation(conv)

        payload = Query(query_keys.public, query).to_bytes()
        async with self.client.post(self.base_url.join(URL('/bb/broadcast')), data=payload) as response:
            response.raise_for_status()