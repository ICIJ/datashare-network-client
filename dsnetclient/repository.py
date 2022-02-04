import abc
from collections import defaultdict
from typing import List, Mapping

from databases import Database
from dsnet.core import Conversation, PigeonHole
from dsnet.message import PigeonHoleMessage
from sqlalchemy import insert, select, column
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
    async def get_conversation_by_key(self, conversation_pub_key: bytes) -> Conversation:
        """
        Gets a conversation by its public key

        :param conversation_pub_key: Conversation public key to retrieve
        :return: Retrieved conversation
        """

    @abc.abstractmethod
    async def get_conversation_by_address(self, address: bytes) -> Conversation:
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
    async def get_pigeonhole(self, address: bytes) -> PigeonHole:
        """
        Get a pigeonhole based on its address.

        :param address: pigeonhole address
        :return: pigeonhole object
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
                                                  created_at=conversation.created_at,
                                                  query=conversation.query))
            for ph in conversation._pigeonholes.values():
                await self.save_pigeonhole(ph, conversation_id)

            for i, msg in enumerate(conversation._messages):
                await self.save_message(msg, conversation_id, i)

    async def save_message(self, message: PigeonHoleMessage, conversation_id: int, message_number: int) -> None:
        stmt = insert(message_table).values(address=message.address,
                                            from_key=message.from_key,
                                            payload=message.payload,
                                            timestamp=message.timestamp,
                                            conversation_id=conversation_id,
                                            message_number=message_number)
        await self.database.execute(stmt)

    async def get_conversation_by_key(self, public_key) -> Conversation:
        stmt = self._create_conversation_statement().where(conversation_table.c.public_key == public_key)
        return (await self.get_conversations(stmt))[0]

    async def get_conversation_by_address(self, address) -> Conversation:
        stmt = self._create_conversation_statement().where(pigeonhole_table.c.address == address)
        return (await self.get_conversations(stmt))[0]

    async def get_conversations_filter_by(self, **kwargs) -> List[Conversation]:
        clauses = [column(key) == value for key, value in kwargs.items()]
        stmt = self._create_conversation_statement().where(*clauses)
        return await self.get_conversations(stmt)

    async def get_conversations(self, statement=None) -> List[Conversation]:
        stmt = self._create_conversation_statement() if statement is None else statement
        conversation_maps = await self.database.fetch_all(stmt)
        return self._merge_conversations(conversation_maps)

    def _create_conversation_statement(self) -> Select:
        return select(conversation_table, pigeonhole_table, message_table).join(pigeonhole_table).outerjoin(message_table)

    def _merge_conversations(self, conversation_maps: List[Mapping]) -> List[Conversation]:
        messages_dict = defaultdict(dict)
        ph_dict = defaultdict(dict)
        conversations = dict()
        for row in conversation_maps:
            conversations[row['id']] = Conversation(row['private_key'], row['other_public_key'], row['nb_sent_messages'], row['nb_recv_messages'],
                                                    row['querier'], row['created_at'], row['query'])
            ph_dict[row['id']][row['address']] = PigeonHole(public_key_for_dh=row['public_key_1'], message_number=row['message_number'],
                           dh_key=row['dh_key'])
            messages_dict[row['id']][row['address_1']] = PigeonHoleMessage(address=row['address_1'], payload=row['payload'], from_key=row['from_key'], timestamp=row['timestamp'])
        return [Conversation(c.private_key, c.other_public_key, c.nb_sent_messages, c.nb_recv_messages, c.querier, c.created_at, c.query, pigeonholes=list(ph_dict[id].values()), messages=list(messages_dict[id].values())) for id, c in conversations.items()]

    async def peers(self) -> List[Peer]:
        stmt = peer_table.select()
        return [Peer(**peer) for peer in await self.database.fetch_all(stmt)]

    async def save_peer(self, peer: Peer):
        await self.database.execute(insert(peer_table).values(public_key=peer.public_key))
