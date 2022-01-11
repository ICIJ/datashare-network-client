import databases
import pytest
from dsnet.core import PigeonHole
from dsnet.crypto import gen_key_pair
from sqlalchemy import create_engine

from dsnetclient.models import metadata
from dsnetclient.repository import SqlalchemyRepository

DATABASE_URL = 'sqlite:///dsnet.db'
database = databases.Database(DATABASE_URL)


@pytest.fixture
async def connect_disconnect_db():
    metadata.create_all(create_engine(DATABASE_URL))
    await database.connect()
    yield
    await database.disconnect()


@pytest.mark.asyncio
async def test_save_pigeonhole(connect_disconnect_db):
    bob_keys = gen_key_pair()
    alice_keys = gen_key_pair()
    ph = PigeonHole(alice_keys.public, bob_keys.private, bob_keys.public)
    repository = SqlalchemyRepository(database)

    assert await repository.save_pigeonhole(ph, 123) is True
    actual_ph = await repository.get_pigeonhole(ph.address)

    assert actual_ph is not None
    assert actual_ph.address == ph.address
    assert actual_ph.public_key == ph.public_key
    assert actual_ph.message_number == ph.message_number
    assert actual_ph.sym_key == ph.sym_key
