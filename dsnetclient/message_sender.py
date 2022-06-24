import asyncio
from abc import ABC, abstractmethod
from asyncio import Queue
from typing import Awaitable, Callable

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


class QueueMessageSender(MessageSender):
    """Message sender which sends messages at interfall following a statistical distribution."""

    def __init__(self, base_url, distribution_fn: Callable[[], float],
                 send_fn: Callable[[PigeonHoleMessage], Awaitable[None]] = None,
                 cover_fn: Callable[[None], PigeonHoleMessage] = None
        ):
        self.base_url = base_url
        self.queue = Queue()
        self._stop_asked = False
        self.send_fn = self._default_send_fn if send_fn is None else send_fn
        self.cover_fn = self._default_cover_fn if cover_fn is None else cover_fn
        self.distribution_fn = distribution_fn
        self._message_sender = asyncio.get_event_loop().create_task(self._send_coroutine())

    async def send(self, message: PigeonHoleMessage) -> None:
        await self.queue.put(message)

    async def _send_coroutine(self):
        while not self._stop_asked:
            await asyncio.sleep(self.distribution_fn())

            try:
                message = self.queue.get_nowait()
                await self.send_fn(message)
            except asyncio.QueueEmpty:
                await self.send_fn(self.cover_fn())

    async def _default_send_fn(self, message: PigeonHoleMessage):
        async with ClientSession() as session:
            async with session.post(self.base_url.join(URL(f'/ph/{message.address.hex()}')), data=message.to_bytes()) as http_response:
                http_response.raise_for_status()

    def _default_cover_fn(self):
        return PigeonHoleMessage(b"address", b"payload", b"key")

    async def stop(self):
        self._stop_asked = True
        await self._message_sender

