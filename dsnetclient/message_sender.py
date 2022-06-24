from abc import ABC, abstractmethod

from aiohttp import ClientSession
from yarl import URL

from dsnet.message import PigeonHoleMessage


class MessageSender(ABC):
    """Message sender"""

    @abstractmethod
    async def send(self, message: PigeonHoleMessage) -> None:
        """Send a message to the pigeon hole."""


class DirectMessageSender(MessageSender):
    """Message sender which sends messages immediately."""

    def __init__(self, base_url):
        self.base_url = base_url

    async def send(self, message: PigeonHoleMessage) -> None:
        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL(f'/ph/{message.address.hex()}')), data=message.to_bytes()) as http_response:
                http_response.raise_for_status()