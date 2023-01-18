from abc import ABC, abstractmethod
from typing import Callable, Optional, Tuple

from aiohttp import ClientSession, ClientTimeout
import msgpack
from dsnet.core import PigeonHole
from dsnet.logger import logger
from yarl import URL

from dsnet.message import PigeonHoleNotification, PigeonHoleMessage

from dsnetclient.repository import Repository


class MessageRetriever(ABC):

    @abstractmethod
    async def retrieve(self, msg: PigeonHoleNotification) -> Optional[Tuple[bytes, PigeonHole]]:
        """Retrieve messages from a notification."""


class AddressMatchMessageRetriever(MessageRetriever):
    def __init__(self, url: URL, repository: Repository) -> None:
        self.base_url = url
        self.repository = repository

    async def retrieve(self, msg: PigeonHoleNotification) -> Optional[Tuple[bytes, PigeonHole]]:
        async with ClientSession() as session:
            addrs = await self.repository.get_pigeonholes_by_adr(msg.adr_hex)
            for ph in addrs:
                logger.debug("Try to retrieve message matching shortened %s", ph.address.hex())
                async with session.get(self.base_url.join(URL(f'/ph/{ph.address.hex()}'))) as http_response:
                    http_response.raise_for_status()
                    return await http_response.read(), ph


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

    async def retrieve(self, msg: PigeonHoleNotification) -> Optional[Tuple[bytes, PigeonHole]]:
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
                        return message_b, ph
        elif self.retrieve_decision_fn():
            async with self.session.get(self.base_url.join(URL(f'/ph/{msg.adr_hex}'))):
                pass
