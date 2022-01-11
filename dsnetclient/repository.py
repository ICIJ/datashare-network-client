import abc

from databases import Database
from dsnet.core import Conversation, PigeonHole
from sqlalchemy import insert

from dsnetclient.models import pigeonhole_table


class Repository(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def save(self, conversation: Conversation) -> bool:
        """
        Saves a response address to a query.

        :param conversation: Conversation to save
        :return: True if conversation is saved, else False
        """

    @abc.abstractmethod
    async def delete_pigeonhole(self, address: bytes) -> bool:
        """
        Delete a pigeon hole given an address.

        :param address: pigeon hole address to remove
        :return: True if pigeon hole is deleted, else False
        """

    @abc.abstractmethod
    async def save_pigeonhole(self, pigeon_hole: PigeonHole, conversation_id: int) -> bool:
        """
        Save a pigeon hole based on a conversation id.

        :param conversation_id: conversation id
        :param pigeon_hole: to save
        :return: True if pigeon hole is saved, else False
        """

    @abc.abstractmethod
    async def get_pigeonhole(self, address: bytes) -> PigeonHole:
        """
        Get a pigeon hole based on its address.

        :param address: pigeon hole address
        :return: pigeonhole object
        """


class SqlalchemyRepository(Repository):
    def __init__(self, database: Database):
        self.database = database

    async def get_pigeonhole(self, address: bytes) -> PigeonHole:
        stmt = pigeonhole_table.select().where(pigeonhole_table.c.address == address)
        row = await self.database.fetch_one(stmt)
        return PigeonHole(public_key_for_dh=row.public_key,
                          message_number=row.message_number,
                          dh_key=row.dh_key)

    async def save_pigeonhole(self, pigeonhole: PigeonHole, conversation_id: int) -> bool:
        stmt = insert(pigeonhole_table).values(address=pigeonhole.address,
                                               dh_key=pigeonhole.dh_key,
                                               public_key=pigeonhole.public_key,
                                               message_number=pigeonhole.message_number,
                                               conversation_id=conversation_id)
        return await self.database.execute(stmt) > 0

    async def delete_pigeonhole(self, address: bytes) -> bool:
        pass

    async def save(self, conversation: Conversation) -> bool:
        pass
