import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from dsnet.message import PigeonHoleMessage
from yarl import URL

from dsnetclient.message_sender import QueueMessageSender


def const_distribution() -> float:
    return 4.0


@pytest.mark.skip()
@pytest.mark.asyncio
@pytest.mark.looptime
async def test_server_called_if_cover_sent():
    send_fn = AsyncMock()
    cover_fn = Mock()
    qms = QueueMessageSender(URL(), const_distribution, send_fn, cover_fn)

    await asyncio.sleep(12)
    await qms.stop()

    send_fn.assert_called()
    cover_fn.assert_called()

    assert send_fn.call_count == cover_fn.call_count == 3


@pytest.mark.skip()
@pytest.mark.asyncio
@pytest.mark.looptime
async def test_server_called_with_distribution_fn_sleep():
    send_fn = AsyncMock()
    qms = QueueMessageSender(URL(), const_distribution, send_fn)

    message = PigeonHoleMessage(b'address', b'payload', b'from_key')
    await qms.send(message)
    while not qms.queue.empty():
        await asyncio.sleep(0.1)

    await qms.stop()

    assert send_fn.call_count == 2
    assert asyncio.get_running_loop().time() == 8.0


