from sqlite3 import IntegrityError

import databases
import pytest
from dsnet.core import PigeonHole, Conversation, Message
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


@pytest.mark.asyncio
async def test_save_conversation_with_a_query(connect_disconnect_db):
    query_keys = gen_key_pair()
    bob_keys = gen_key_pair()

    conversation = Conversation(query_keys.private, bob_keys.public, query='France', querier=True)

    repository = SqlalchemyRepository(database)
    await repository.save_conversation(conversation)
    actual_conversation = await repository.get_conversation_by_key(conversation.public_key)

    assert actual_conversation is not None
    assert actual_conversation.private_key == conversation.private_key
    assert actual_conversation.public_key == conversation.public_key
    assert actual_conversation.other_public_key == conversation.other_public_key
    assert actual_conversation.querier == conversation.querier
    assert actual_conversation.query == conversation.query
    assert actual_conversation.nb_sent_messages == conversation.nb_sent_messages
    assert actual_conversation.nb_recv_messages == conversation.nb_recv_messages
    assert actual_conversation.is_receiving(conversation.last_address) == True
    assert await repository.get_pigeonhole(actual_conversation.last_address) is not None


@pytest.mark.asyncio
async def test_save_conversation_with_messages(connect_disconnect_db):
    repository = SqlalchemyRepository(database)

    query_keys = gen_key_pair()
    alicia_keys = gen_key_pair()
    conversation = Conversation(query_keys.private, alicia_keys.public, query='Pop', querier=True)
    ph = conversation.pigeonhole_for_address(conversation.last_address)
    encrypted_message1 = ph.encrypt('alicia response1')
    conversation.add_message(Message(conversation.last_address, encrypted_message1, alicia_keys.public))
    ph = conversation.pigeonhole_for_address(conversation.last_address)
    encrypted_message2 = ph.encrypt('alicia response2')
    conversation.add_message(Message(conversation.last_address, encrypted_message2, alicia_keys.public))

    await repository.save_conversation(conversation)

    actual_conversation = await repository.get_conversation_by_key(conversation.public_key)
    assert len(actual_conversation._messages) == 2