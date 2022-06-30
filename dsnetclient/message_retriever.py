from abc import ABC, abstractmethod

from aiohttp import ClientSession
from yarl import URL

from dsnet.message import PigeonHoleNotification, PigeonHoleMessage
from dsnet.logger import logger

from dsnetclient.repository import Repository


class MessageRetriever(ABC):

    @abstractmethod
    async def retrieve(self, msg: PigeonHoleNotification) -> None:
        """Retrieve messages from a notification."""


class ExactMatchMessageRetriever(MessageRetriever):

    def __init__(self, url: URL, repository: Repository) -> None:
        self.base_url = url
        self.repository = repository


    async def retrieve(self, msg: PigeonHoleNotification) -> None:
        async with ClientSession() as session:
            for ph in await self.repository.get_pigeonholes_by_adr(msg.adr_hex):
                async with session.get(self.base_url.join(URL(f'/ph/{ph.address.hex()}'))) as http_response:
                    http_response.raise_for_status()
                    message = PigeonHoleMessage.from_bytes(await http_response.read())
                    message.from_key = ph.key_for_hash
                    conversation = await self.repository.get_conversation_by_address(ph.address)
                    logger.debug(f"adding message {message.address.hex()} to conversation {conversation.id}")
                    conversation.add_message(message)
                    await self.repository.save_conversation(conversation)

