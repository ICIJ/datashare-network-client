from aiohttp import ClientSession

from yarl import URL


class DsnetApi:

    def __init__(self, url: URL) -> None:
        self.base_url = url
        self.client = ClientSession()

    async def get_server_version(self):
        async with self.client.get(self.base_url) as resp:
            return await resp.json()
