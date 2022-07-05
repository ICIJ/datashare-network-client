from collections import namedtuple
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientSession
from dsnet.core import PigeonHole
from dsnet.message import PigeonHoleNotification
from yarl import URL

from dsnetclient.message_retriever import ProbabilisticCoverMessageRetriever
from dsnetclient.repository import Repository

@pytest.mark.asyncio
async def test_server_called_when_not_waiting_for_an_address_and_retrieval_fn_returns_true():
    httpsession = AsyncMock(ClientSession)
    retriever = ProbabilisticCoverMessageRetriever(URL(), AsyncMock(Repository), lambda: True, httpsession)
    await retriever.retrieve(PigeonHoleNotification('beefca'))

    httpsession.get.assert_called()


@pytest.mark.asyncio
async def test_server_not_called_when_not_waiting_for_an_address_and_retrieval_fn_returns_false():
    httpsession = AsyncMock(ClientSession)
    retriever = ProbabilisticCoverMessageRetriever(URL(), AsyncMock(Repository), lambda: False, httpsession)
    await retriever.retrieve(PigeonHoleNotification('beefca'))

    httpsession.get.assert_not_called()