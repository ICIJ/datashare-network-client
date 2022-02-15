from datetime import datetime

import databases
import pytest
import pytest_asyncio
from dsnet.core import PigeonHole, Conversation
from dsnet.crypto import gen_key_pair
from dsnet.message import PigeonHoleMessage, PigeonHoleNotification
from sqlalchemy import create_engine

from dsnetclient.models import metadata
from dsnetclient.repository import SqlalchemyRepository, Peer

DATABASE_URL = 'sqlite:///dsnet.db'
database = databases.Database(DATABASE_URL)


@pytest_asyncio.fixture
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
    ph = PigeonHole(alice_keys.public, bob_keys.secret, bob_keys.public)
    repository = SqlalchemyRepository(database)

    await repository.save_pigeonhole(ph, 123)
    actual_ph = await repository.get_pigeonhole(ph.address)

    assert actual_ph is not None
    assert actual_ph.address == ph.address
    assert actual_ph.dh_key == ph.dh_key
    assert actual_ph.key_for_hash == ph.key_for_hash
    assert actual_ph.message_number == ph.message_number
    assert actual_ph.sym_key == ph.sym_key


@pytest.mark.asyncio
async def test_get_pigeonhole_by_adr(connect_disconnect_db):
    bob_keys = gen_key_pair()
    alice_keys = gen_key_pair()
    ph = PigeonHole(alice_keys.public, bob_keys.secret, bob_keys.public)
    repository = SqlalchemyRepository(database)

    await repository.save_pigeonhole(ph, 123)
    await repository.save_pigeonhole(PigeonHole(gen_key_pair().public, bob_keys.secret, bob_keys.public), 123)
    phs = await repository.get_pigeonholes_by_adr(PigeonHoleNotification.from_address(ph.address).adr_hex)

    assert len(phs) == 1
    assert phs[0].address == ph.address


@pytest.mark.asyncio
async def test_save_pigeonhole_idempotency(connect_disconnect_db):
    bob_keys = gen_key_pair()
    alice_keys = gen_key_pair()
    ph = PigeonHole(alice_keys.public, bob_keys.secret, bob_keys.public)
    repository = SqlalchemyRepository(database)

    await repository.save_pigeonhole(ph, 123)
    await repository.save_pigeonhole(ph, 123)
    assert await repository.get_pigeonhole(ph.address) is not None


@pytest.mark.asyncio
async def test_delete_pigeonhole(connect_disconnect_db):
    bob_keys = gen_key_pair()
    alice_keys = gen_key_pair()
    ph = PigeonHole(alice_keys.public, bob_keys.secret, bob_keys.public)
    repository = SqlalchemyRepository(database)
    await repository.save_pigeonhole(ph, 123)

    assert await repository.delete_pigeonhole(ph.address) is True
    assert await repository.get_pigeonhole(ph.address) is None


@pytest.mark.asyncio
async def test_save_conversation(connect_disconnect_db):
    query_keys = gen_key_pair()
    bob_keys = gen_key_pair()
    conversation = Conversation.create_from_querier(query_keys.secret, bob_keys.public, query=b'France')
    conversation.created_at = datetime(2022, 1, 2, 3, 4, 5)

    repository = SqlalchemyRepository(database)
    await repository.save_conversation(conversation)
    actual_conversation = await repository.get_conversation_by_key(conversation.public_key)

    assert actual_conversation is not None
    assert actual_conversation.secret_key == conversation.secret_key
    assert actual_conversation.public_key == conversation.public_key
    assert actual_conversation.other_public_key == conversation.other_public_key
    assert actual_conversation.querier == conversation.querier
    assert actual_conversation.query == conversation.query
    assert actual_conversation.is_receiving(conversation.last_address)
    assert actual_conversation.created_at == conversation.created_at
    assert await repository.get_pigeonhole(actual_conversation.last_address) is not None


@pytest.mark.asyncio
async def test_save_conversation_with_messages(connect_disconnect_db):
    repository = SqlalchemyRepository(database)

    query_keys = gen_key_pair()
    alicia_keys = gen_key_pair()
    conversation = Conversation.create_from_querier(query_keys.secret, alicia_keys.public, query=b'Pop')
    ph = conversation.pigeonhole_for_address(conversation.last_address)
    encrypted_message1 = ph.encrypt(b'alicia response1')
    conversation.add_message(PigeonHoleMessage(conversation.last_address, encrypted_message1, alicia_keys.public))
    ph = conversation.pigeonhole_for_address(conversation.last_address)
    encrypted_message2 = ph.encrypt(b'alicia response2')
    conversation.add_message(PigeonHoleMessage(conversation.last_address, encrypted_message2, alicia_keys.public))

    await repository.save_conversation(conversation)

    actual_conversation = await repository.get_conversation_by_key(conversation.public_key)
    assert actual_conversation.nb_recv_messages == 2
    assert actual_conversation.nb_sent_messages == 1


@pytest.mark.asyncio
async def test_get_conversation_by_key_no_records(connect_disconnect_db):
    assert await SqlalchemyRepository(database).get_conversation_by_key(b'unknown') is None


@pytest.mark.asyncio
async def test_get_conversation_by_address_no_records(connect_disconnect_db):
    assert await SqlalchemyRepository(database).get_conversation_by_address(b'unknown') is None


@pytest.mark.asyncio
async def test_get_conversation_by_address(connect_disconnect_db):
    query_keys = gen_key_pair()
    carol_keys = gen_key_pair()

    conversation = Conversation.create_from_querier(query_keys.secret, carol_keys.public, query=b'France')

    repository = SqlalchemyRepository(database)
    await repository.save_conversation(conversation)

    assert await repository.get_conversation_by_address(conversation.last_address) is not None


@pytest.mark.asyncio
async def test_get_conversations(connect_disconnect_db):
    query_keys = gen_key_pair()
    carol_keys = gen_key_pair()

    conversation = Conversation.create_from_querier(query_keys.secret, carol_keys.public, query=b'Hello')

    repository = SqlalchemyRepository(database)
    await repository.save_conversation(conversation)

    assert len(await repository.get_conversations()) == 1
    assert (await repository.get_conversations())[0].query == b'Hello'


@pytest.mark.asyncio
async def test_get_conversation_buy_id(connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    query_keys = gen_key_pair()
    carol_keys = gen_key_pair()
    conversation = Conversation.create_from_querier(query_keys.secret, carol_keys.public, query=b'Hello')
    await repository.save_conversation(conversation)

    assert await repository.get_conversation(1) is not None


@pytest.mark.asyncio
async def test_get_conversations_filter_by_properties(connect_disconnect_db):
    query_keys = gen_key_pair()
    carol_keys = gen_key_pair()

    conversation = Conversation.create_from_querier(query_keys.secret, carol_keys.public, query=b'Hello')

    repository = SqlalchemyRepository(database)
    await repository.save_conversation(conversation)

    assert len(await repository.get_conversations_filter_by(querier=True)) == 1


@pytest.mark.asyncio
async def test_get_conversations_with_messages(connect_disconnect_db):
    query_keys = gen_key_pair()
    carol_keys = gen_key_pair()
    carol_side = Conversation.create_from_recipient(carol_keys.secret, query_keys.public)
    querier_side = Conversation.create_from_querier(query_keys.secret, carol_keys.public, query=b'Hello')
    querier_side.add_message(carol_side.create_response(b"Hi"))

    repository = SqlalchemyRepository(database)
    await repository.save_conversation(querier_side)

    conversations = await repository.get_conversations()
    assert len(conversations) == 1
    assert conversations[0].query == b'Hello'
    assert len(conversations[0]._pigeonholes) == 1
    assert len(conversations[0]._messages) == 2
    assert conversations[0].last_message.payload == b'Hi'


@pytest.mark.asyncio
async def test_save_conversation_deletes_old_pigeonholes(connect_disconnect_db):
    repository = SqlalchemyRepository(database)
    query_keys = gen_key_pair()
    carol_keys = gen_key_pair()
    carol_side = Conversation.create_from_recipient(carol_keys.secret, query_keys.public)
    querier_side = Conversation.create_from_querier(query_keys.secret, carol_keys.public, query=b'Hello')
    await repository.save_conversation(querier_side)
    querier_side = (await repository.get_conversations())[0]
    querier_side.add_message(carol_side.create_response(b"Hi"))

    await repository.save_conversation(querier_side)

    conversations = await repository.get_conversations()
    assert len(conversations) == 1
    assert len(conversations[0]._pigeonholes) == 1


@pytest.mark.asyncio
async def test_save_get_peers(connect_disconnect_db):
    peer_keys = gen_key_pair()
    repository = SqlalchemyRepository(database)
    await repository.save_peer(Peer(peer_keys.public))

    actual_peers = await repository.peers()
    assert len(actual_peers) == 1
    assert actual_peers[0].public_key == peer_keys.public


@pytest.mark.asyncio
async def test_save_peers_twice(connect_disconnect_db):
    peer_keys = gen_key_pair()
    repository = SqlalchemyRepository(database)
    await repository.save_peer(Peer(peer_keys.public))
    await repository.save_peer(Peer(peer_keys.public))
    assert len(await repository.peers()) == 1

