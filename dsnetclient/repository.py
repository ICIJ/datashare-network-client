import abc
import sqlite3
from collections import defaultdict
from typing import List, Mapping, Optional

from databases import Database
from dsnet.core import Conversation, PigeonHole
from dsnet.logger import logger
from dsnet.message import PigeonHoleMessage, PigeonHoleNotification
from sqlalchemy import insert, select, column, delete
from sqlalchemy.sql import Select

from dsnetclient.models import pigeonhole_table, conversation_table, message_table, peer_table


class Peer:
    def __init__(self, public_key: bytes, id=None):
        self.id = id
        self.public_key = public_key


class Repository(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def get_conversations_filter_by(self, **kwargs) -> List[Conversation]:
        """
        Get database conversation list filtered by some Conversation properties

        :param kwargs:
        :return: Conversation list
        """

    @abc.abstractmethod
    async def get_conversation(self, id: int) -> Optional[Conversation]:
        """
        Get database conversation list filtered by some Conversation properties

        :param id: conversation id
        :return: Conversation if found
        """

    @abc.abstractmethod
    async def get_conversation_by_key(self, conversation_pub_key: bytes) -> Optional[Conversation]:
        """
        Gets a conversation by its public key

        :param conversation_pub_key: Conversation public key to retrieve
        :return: Retrieved conversation
        """

    @abc.abstractmethod
    async def get_conversation_by_address(self, address: bytes) -> Optional[Conversation]:
        """
        Gets a conversation by ph address

        :param address: last pigeon hole address linked to conversation
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
    async def get_pigeonhole(self, address: bytes) -> Optional[PigeonHole]:
        """
        Get a pigeonhole based on its address.

        :param address: pigeonhole address
        :return: pigeonhole object
        """

    @abc.abstractmethod
    async def get_pigeonholes(self) -> List[PigeonHole]:
        """
        Listening pigeonholes

        :return: pigeonhole list
        """

    @abc.abstractmethod
    async def get_conversations(self) -> List[Conversation]:
        """
        Get database conversation list

        :return: list of conversation objects
        """

    @abc.abstractmethod
    async def peers(self) -> List[Peer]:
        """
        Get peers list

        :return: list of peers
        """

    @abc.abstractmethod
    async def save_peer(self, peer: Peer) -> None:
        """
        Save peer
        """

    @abc.abstractmethod
    async def get_pigeonholes_by_adr(self, adr: str) -> List[PigeonHole]:
        """
        get the list of pigeonholes beginning with adr

        :param adr: short pigeon hole address
        :return: list of matching pigeonholes
        """


class SqlalchemyRepository(Repository):
    def __init__(self, database: Database):
        self.database = database

    @staticmethod
    def _pigeonhole_from_row(row) -> PigeonHole:
        return PigeonHole(
            dh_key=row['dh_key'],
            message_number=row['message_number'],
            key_for_hash=row['key_for_hash'],
            conversation_id=row['conversation_id'],
        )

    async def get_pigeonholes(self) -> List[PigeonHole]:
        return [SqlalchemyRepository._pigeonhole_from_row(row)
             for row in await self.database.fetch_all(pigeonhole_table.select())]

    async def get_conversation(self, id: int) -> Optional[Conversation]:
        stmt = self._create_conversation_statement().where(conversation_table.c.id == id)
        return await self.get_one_conversation(stmt)

    async def get_pigeonholes_by_adr(self, adr_hex: str) -> List[PigeonHole]:
        stmt = pigeonhole_table.select().where(pigeonhole_table.c.adr_hex == adr_hex)
        rows = await self.database.fetch_all(stmt)
        return [
            SqlalchemyRepository._pigeonhole_from_row(row)
            for row in rows
        ]

    async def get_pigeonhole(self, address: bytes) -> Optional[PigeonHole]:
        stmt = pigeonhole_table.select().where(pigeonhole_table.c.address == address)
        row = await self.database.fetch_one(stmt)
        return SqlalchemyRepository._pigeonhole_from_row(row) if row is not None else None

    async def save_pigeonhole(self, pigeonhole: PigeonHole, conversation_id: int) -> None:
        try:
            await self.database.execute(insert(pigeonhole_table).values(
                address=pigeonhole.address,
                adr_hex=PigeonHoleNotification.from_address(pigeonhole.address).adr_hex,
                dh_key=pigeonhole.dh_key,
                key_for_hash=pigeonhole.key_for_hash,
                message_number=pigeonhole.message_number,
                conversation_id=conversation_id
                )
            )
        except sqlite3.IntegrityError:
            logger.debug("Attempted to add an existing pigeonhole")

    async def delete_pigeonhole(self, address: bytes) -> bool:
        return await self.database.execute(pigeonhole_table.delete().where(pigeonhole_table.c.address == address)) > 0

    async def save_conversation(self, conversation: Conversation) -> None:
        async with self.database.transaction():
            if conversation.id is None:
                conversation_id = await self.database.execute(
                    insert(conversation_table).values(
                        secret_key=conversation.secret_key,
                        public_key=conversation.public_key,
                        other_public_key=conversation.other_public_key,
                        querier=conversation.querier,
                        created_at=conversation.created_at,
                        query=conversation.query
                    )
                )
            else:
                conversation_id = conversation.id
                conversation_addresses = set(conversation._pigeonholes.keys())
                addresses_in_db = set(
                    row[0] for row in await self.database.fetch_all(
                        select(pigeonhole_table.c.address).where(pigeonhole_table.c.conversation_id == conversation_id)
                    )
                )
                diff = addresses_in_db - conversation_addresses

                await self.database.execute(delete(pigeonhole_table).where(pigeonhole_table.c.address.in_(list(diff))))
            for ph in conversation._pigeonholes.values():
                await self.save_pigeonhole(ph, conversation_id)

            for msg in conversation._messages:
                await self._save_message(msg, conversation_id)

    async def _save_message(self, message: PigeonHoleMessage, conversation_id: int) -> None:
        try:
            stmt = insert(message_table).values(
                address=message.address,
                from_key=message.from_key,
                payload=message.payload,
                timestamp=message.timestamp,
                conversation_id=conversation_id
            )
            await self.database.execute(stmt)
        except sqlite3.IntegrityError:
            logger.debug("Attempted to save an existing message")

    async def get_conversation_by_key(self, public_key) -> Optional[Conversation]:
        stmt = self._create_conversation_statement().where(conversation_table.c.public_key == public_key)
        return await self.get_one_conversation(stmt)

    async def get_conversation_by_address(self, address) -> Optional[Conversation]:
        stmt = self._create_conversation_statement().where(pigeonhole_table.c.address == address)
        return await self.get_one_conversation(stmt)

    async def get_conversations_filter_by(self, **kwargs) -> List[Conversation]:
        clauses = [column(key) == value for key, value in kwargs.items()]
        stmt = self._create_conversation_statement().where(*clauses)
        return await self.get_conversations(stmt)

    async def get_one_conversation(self, stmt) -> Optional[Conversation]:
        convs = await self.get_conversations(stmt)
        return convs[0] if convs else None

    async def get_conversations(self, statement=None) -> List[Conversation]:
        stmt = self._create_conversation_statement() if statement is None else statement
        conversation_maps = await self.database.fetch_all(stmt)
        return self._merge_conversations(conversation_maps)

    def _create_conversation_statement(self) -> Select:
        return select(conversation_table, pigeonhole_table, message_table).outerjoin(pigeonhole_table).join(
            message_table)

    def _merge_conversations(self, conversation_maps: List[Mapping]) -> List[Conversation]:
        messages_dict = defaultdict(dict)
        ph_dict = defaultdict(dict)
        conversations = dict()
        for row in conversation_maps:
            conversations[row['id']] = Conversation(
                row['secret_key'],
                row['other_public_key'],
                row['querier'],
                row['created_at'],
                row['query'],
                id=row['id']
            )
            if row['dh_key']:
                ph_dict[row['id']][row['address']] = PigeonHole(
                    message_number=row['message_number'],
                    dh_key=row['dh_key'],
                    key_for_hash=row['key_for_hash'],
                    conversation_id=row['id'],
                )
            messages_dict[row['id']][row['address_1']] = PigeonHoleMessage(
                address=row['address_1'],
                payload=row['payload'],
                from_key=row['from_key'],
                timestamp=row['timestamp'],
                conversation_id=row['id']
            )
        return [
            Conversation(
                c.secret_key,
                c.other_public_key,
                c.querier,
                c.created_at,
                c.query,
                pigeonholes=list(ph_dict[id].values()),
                messages=list(messages_dict[id].values()),
                id=c.id
            )
            for id, c in conversations.items()
        ]

    async def peers(self) -> List[Peer]:
        stmt = peer_table.select()
        return [Peer(**peer) for peer in await self.database.fetch_all(stmt)]

    async def save_peer(self, peer: Peer):
        try:
            await self.database.execute(insert(peer_table).values(public_key=peer.public_key))
        except sqlite3.IntegrityError:
            logger.debug("Attempted to save an existing peer")
