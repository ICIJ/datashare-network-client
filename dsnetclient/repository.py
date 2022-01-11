import abc
import logging

from databases import Database
from dsnet.core import Conversation, PigeonHole
from sqlalchemy import insert
from sqlalchemy.exc import DatabaseError, IntegrityError

from dsnetclient.models import pigeonhole_table


class Repository(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def save_conversation(self, conversation: Conversation) -> bool:
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

    async def save_conversation(self, conversation: Conversation) -> bool:
        pass
