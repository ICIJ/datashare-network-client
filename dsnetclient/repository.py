import abc
from datetime import datetime

from databases import Database
from dsnet.core import Conversation, PigeonHole, Message
from sqlalchemy import insert

from dsnetclient.models import pigeonhole_table, conversation_table, message_table


class Repository(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_conversation_by_key(self, conversation_pub_key: bytes) -> Conversation:
        """
        Saves a response address to a query.

        :param conversation_pub_key: Conversation public key to retrieve
        :return: Retrieved conversation
        """

    @abc.abstractmethod
    async def save_conversation(self, conversation: Conversation) -> None:
        """
        Saves a response address to a query.

        :param conversation: Conversation to save
        :return: True if conversation is saved, else False
        """

    @abc.abstractmethod
    async def delete_pigeonhole(self, address: bytes) -> bool:
        """
        Delete a pigeonhole given an address.

        :param address: pigeonhole address to remove
        :return: True if pigeonhole is deleted, else False
        """

    @abc.abstractmethod
    async def save_pigeonhole(self, pigeon_hole: PigeonHole, conversation_id: int) -> None:
        """
        Save a pigeonhole based on a conversation id.

        :param conversation_id: conversation id
        :param pigeon_hole: to save
        :return: True if pigeonhole is saved, else False
        """

    @abc.abstractmethod
    async def get_pigeonhole(self, address: bytes) -> PigeonHole:
        """
        Get a pigeonhole based on its address.

        :param address: pigeonhole address
        :return: pigeonhole object
        """


class SqlalchemyRepository(Repository):
    def __init__(self, database: Database):
        self.database = database

    async def get_pigeonhole(self, address: bytes) -> PigeonHole:
        stmt = pigeonhole_table.select().where(pigeonhole_table.c.address == address)
        row = await self.database.fetch_one(stmt)
        return PigeonHole(public_key_for_dh=row['public_key'],
                          message_number=row['message_number'],
                          dh_key=row['dh_key']) if row is not None else None

    async def save_pigeonhole(self, pigeonhole: PigeonHole, conversation_id: int) -> None:
        await self.database.execute(insert(pigeonhole_table).values(address=pigeonhole.address,
                                                                    dh_key=pigeonhole.dh_key,
                                                                    public_key=pigeonhole.public_key,
                                                                    message_number=pigeonhole.message_number,
                                                                    conversation_id=conversation_id))

    async def delete_pigeonhole(self, address: bytes) -> bool:
        return await self.database.execute(pigeonhole_table.delete().where(pigeonhole_table.c.address == address)) > 0

    async def save_conversation(self, conversation: Conversation) -> None:
        async with self.database.transaction():
            conversation_id = await self.database.execute(
                insert(conversation_table).values(private_key=conversation.private_key,
                                                  public_key=conversation.public_key,
                                                  other_public_key=conversation.other_public_key,
                                                  nb_sent_messages=conversation.nb_sent_messages,
                                                  nb_recv_messages=conversation.nb_recv_messages,
                                                  querier=conversation.querier,
                                                  created_at=datetime.now(),
                                                  query=conversation.query))
            for ph in conversation._pigeonholes.values():
                await self.save_pigeonhole(ph, conversation_id)

            for i, msg in enumerate(conversation._messages):
                await self.save_message(msg, conversation_id, i)

    async def save_message(self, message: Message, conversation_id: int, message_number: int) -> None:
        stmt = insert(message_table).values(address=message.address,
                                            from_key=message.from_key,
                                            payload=message.payload,
                                            timestamp=message.timestamp,
                                            conversation_id=conversation_id,
                                            message_number=message_number)
        await self.database.execute(stmt)

    async def get_conversation_by_key(self, public_key) -> Conversation:
        stmt = conversation_table.select().where(conversation_table.c.public_key == public_key)
        row = await self.database.fetch_one(stmt)
        phs = [
            PigeonHole(public_key_for_dh=row['public_key'], message_number=row['message_number'], dh_key=row['dh_key'])
            for row in await self.database.fetch_all(
                pigeonhole_table.select().where(pigeonhole_table.c.conversation_id == row['id']))]
        msgs = [Message(address=row['address'], payload=row['payload'], from_key=row['from_key'],
                        timestamp=row['timestamp'])
                for row in await self.database.fetch_all(
                message_table.select().where(message_table.c.conversation_id == row['id']))]

        return Conversation(row['private_key'], row['other_public_key'], row['query'], row['querier'], pigeonholes=phs, messages=msgs)
