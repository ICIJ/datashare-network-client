from sqlite3 import IntegrityError

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
    engine = create_engine(DATABASE_URL)
    metadata.create_all(engine)
    await database.connect()
    yield
    metadata.drop_all(engine)
    await database.disconnect()


@pytest.mark.asyncio
async def test_save_pigeonhole(connect_disconnect_db):
    bob_keys = gen_key_pair()
    alice_keys = gen_key_pair()
    ph = PigeonHole(alice_keys.public, bob_keys.private, bob_keys.public)
    repository = SqlalchemyRepository(database)

    await repository.save_pigeonhole(ph, 123)
    actual_ph = await repository.get_pigeonhole(ph.address)

    assert actual_ph is not None
    assert actual_ph.address == ph.address
    assert actual_ph.public_key == ph.public_key
    assert actual_ph.message_number == ph.message_number
    assert actual_ph.sym_key == ph.sym_key


@pytest.mark.asyncio
async def test_save_pigeonhole_twice(connect_disconnect_db):
    bob_keys = gen_key_pair()
    alice_keys = gen_key_pair()
    ph = PigeonHole(alice_keys.public, bob_keys.private, bob_keys.public)
    repository = SqlalchemyRepository(database)

    await repository.save_pigeonhole(ph, 123)
    with pytest.raises(IntegrityError):
        await repository.save_pigeonhole(ph, 123)


@pytest.mark.asyncio
async def test_delete_pigeonhole(connect_disconnect_db):
    bob_keys = gen_key_pair()
    alice_keys = gen_key_pair()
    ph = PigeonHole(alice_keys.public, bob_keys.private, bob_keys.public)
    repository = SqlalchemyRepository(database)
    await repository.save_pigeonhole(ph, 123)

    assert await repository.delete_pigeonhole(ph.address) is True
    assert await repository.get_pigeonhole(ph.address) is None
