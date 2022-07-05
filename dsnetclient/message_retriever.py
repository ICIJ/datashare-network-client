from abc import ABC, abstractmethod
from typing import Callable, Optional

from aiohttp import ClientSession, ClientTimeout
import msgpack
from yarl import URL

from dsnet.message import PigeonHoleNotification, PigeonHoleMessage
from dsnet.logger import logger

from dsnetclient.repository import Repository


class MessageRetriever(ABC):

    @abstractmethod
    async def retrieve(self, msg: PigeonHoleNotification) -> None:
        """Retrieve messages from a notification."""


class AddressMatchMessageRetriever(MessageRetriever):
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


class ProbabilisticCoverMessageRetriever(MessageRetriever):
    def __init__(
            self,
            url: URL,
            repository: Repository,
            retrieve_decision_fn: Callable[[], bool],
            session: Optional[ClientSession] = None
            ) -> None:
        self.base_url = url
        self.repository = repository
        self.session = ClientSession(timeout=ClientTimeout(total=60)) if session is None else session
        self.retrieve_decision_fn = retrieve_decision_fn

    async def retrieve(self, msg: PigeonHoleNotification) -> None:
        pigeonholes = await self.repository.get_pigeonholes_by_adr(msg.adr_hex)
        if len(pigeonholes) > 0:
            pigeonholes_by_address = {ph.address: ph for ph in pigeonholes}
            async with self.session.get(self.base_url.join(URL(f'/ph/{msg.adr_hex}'))) as http_response:
                http_response.raise_for_status()
                for message_b in msgpack.unpackb(await http_response.read()):
                    message = PigeonHoleMessage.from_bytes(message_b)
                    ph = pigeonholes_by_address.get(message.address)
                    if ph is not None:
                        message.from_key = ph.key_for_hash
                        conversation = await self.repository.get_conversation_by_address(ph.address)
                        logger.debug(f"adding message {message.address.hex()} to conversation {conversation.id}")
                        conversation.add_message(message)
                        await self.repository.save_conversation(conversation)
        elif self.retrieve_decision_fn():
            async with self.session.get(self.base_url.join(URL(f'/ph/{msg.adr_hex}'))):
                pass
