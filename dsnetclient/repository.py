import abc
import datetime
import sqlite3
from collections import defaultdict
from operator import attrgetter
from typing import List, Mapping, Optional

from cryptography.hazmat.primitives._serialization import Encoding, PrivateFormat, NoEncryption
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from databases import Database
from dsnet.core import Conversation, PigeonHole
from dsnet.logger import logger
from dsnet.message import PigeonHoleMessage, PigeonHoleNotification, PublicationMessage
from dsnet.token import AbeToken
from petlib.bn import Bn
from sqlalchemy import insert, select, column, delete, desc
from sqlalchemy.sql import Select
from sscred import packb, unpackb, AbePublicKey
from sqlalchemy.sql.expression import func

from dsnetclient.models import pigeonhole_table, conversation_table, message_table, peer_table, serverkey_table, \
    token_table, parameter_table, publication_table, publication_message_table


class Peer:
    def __init__(self, public_key: bytes, id=None):
        self.id = id
        self.public_key = public_key


class Publication:
    def __init__(self, secret_key: bytes, secret: Bn, nym: str, nb_docs: int, created_at: Optional[datetime.datetime] = None, id = None):
        self.secret = secret
        self.secret_key = secret_key
        self.id = id
        self.created_at = created_at
        self.nb_docs = nb_docs
        self.nym = nym


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

    @abc.abstractmethod
    async def save_token_server_key(self, master_public_key: AbePublicKey) -> bool:
        """
        Save master public key into the repository
        :param master_public_key: token server master public key
        :return: True if saved else False
        """

    @abc.abstractmethod
    async def get_token_server_key(self) -> AbePublicKey:
        """
        Get the master public key from the repository
        :return: Public key bytes
        """

    @abc.abstractmethod
    async def save_tokens(self, tokens: List[AbeToken]) -> int:
        """
        Save query tokens
        :return: True if saved else False
        """

    @abc.abstractmethod
    async def pop_token(self) -> AbeToken:
        """
        pop a query token from the database
        :return: token binary
        """

    @abc.abstractmethod
    async def get_tokens(self) -> List[AbeToken]:
        """
        show stored tokens
        :return: list of token binary
        """

    @abc.abstractmethod
    async def get_last_message_timestamp(self) -> datetime.datetime:
        """
        :return: the last message timestamp
        """

    @abc.abstractmethod
    async def set_parameter(self, key: str, value: str) -> None:
        """
        sets a parameter key/value in database
        """

    @abc.abstractmethod
    async def get_parameter(self, key: str) -> str:
        """
        gets a parameter from its key
        """

    @abc.abstractmethod
    async def get_publications(self) -> List[Publication]:
        """
        :return: the list of all publications
        """
    
    @abc.abstractmethod
    async def save_publication(self, publication: Publication) -> None:
        """
        Save a publication
        :param publication: the publication to save
        """

    @abc.abstractmethod
    async def get_publication_messages(self) -> List[PublicationMessage]:
        """
        :return: the list of all received publication messages order by date
        """

    @abc.abstractmethod
    async def get_publication_message(self, public_key) -> List[PublicationMessage]:
        """
        :return: publication messages by its public key
        """

    @abc.abstractmethod
    async def save_publication_message(self, publication_message: PublicationMessage) -> None:
        """
        save a publication message in database
        """




class SqlalchemyRepository(Repository):
    def __init__(self, database: Database):
        self.database = database
    async def get_last_message_timestamp(self) -> Optional[datetime.datetime]:
        stmt = select(func.max(message_table.c.timestamp)).select_from(message_table)
        record_or_none = await self.database.fetch_one(stmt)
        return record_or_none[0] if record_or_none else None

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
                messages=list(sorted(messages_dict[id].values(), key=attrgetter('timestamp'))),
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

    async def save_token_server_key(self, public_key: AbePublicKey) -> bool:
        stmt = insert(serverkey_table).values(
            master_key=packb(public_key),
            timestamp=datetime.datetime.utcnow(),
        )
        ret = await self.database.execute(stmt)
        return ret > 0

    async def get_token_server_key(self) -> AbePublicKey:
        stmt = serverkey_table.select().order_by(desc(serverkey_table.c.timestamp)).limit(1)
        row = await self.database.fetch_one(stmt)
        return unpackb(row['master_key']) if row else None

    async def save_tokens(self, tokens: List[AbeToken]) -> int:
        data = [
            {
                "token": packb(abe_token.token),
                "secret_key": abe_token.secret_key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()),
                "timestamp": datetime.datetime.utcnow()
            } for abe_token in tokens
        ]
        stmt = insert(token_table).values(data)
        return await self.database.execute(stmt)

    async def pop_token(self) -> Optional[AbeToken]:
        async with self.database.transaction():
            first = token_table.select().order_by(desc(token_table.c.timestamp)).limit(1)
            row = await self.database.fetch_one(first)
            if not row:
                return None
            stmt = token_table.delete().where(token_table.c.token == row["token"])
            await self.database.execute(stmt)
            return AbeToken(Ed25519PrivateKey.from_private_bytes(row["secret_key"]), unpackb(row['token']))

    async def get_tokens(self) -> List[AbeToken]:
        return [AbeToken(Ed25519PrivateKey.from_private_bytes(r['secret_key']), unpackb(r['token']))
                for r in await self.database.fetch_all(token_table.select())]

    async def set_parameter(self, key, value):
        stmt = insert(parameter_table).values({'key': key, 'value': value})
        return await self.database.execute(stmt)

    async def get_parameter(self, key):
        stmt = parameter_table.select().where(parameter_table.c.key==key)
        row = await self.database.fetch_one(stmt)
        return row['value'] if row is not None else None

    async def get_publications(self) -> List[Publication]:
        return [Publication(row['secret_key'], Bn.from_binary(row['secret']), row['nym'], row['nb_docs'], row['created_at'], row['id']) for row in await self.database.fetch_all(publication_table.select().order_by(desc(publication_table.c.created_at)))]

    async def save_publication(self, publication: Publication) -> None:
        data = {
            "secret_key": publication.secret_key,
            "secret": publication.secret.binary(),
            "nym": publication.nym,
            "nb_docs": publication.nb_docs,
            "created_at": datetime.datetime.utcnow()
        }
        stmt = insert(publication_table).values(data)
        return await self.database.execute(stmt)

    async def get_publication_message(self, public_key: bytes) -> List[PublicationMessage]:
        stmt = publication_message_table.select().where(publication_message_table.c.public_key == public_key).\
            order_by(desc(publication_message_table.c.created_at))
        return [create_publication_message(row) for row in await self.database.fetch_all(stmt)]

    async def get_publication_messages(self) -> List[PublicationMessage]:
        stmt = publication_message_table.select().order_by(desc(publication_message_table.c.created_at))
        return [create_publication_message(row) for row in await self.database.fetch_all(stmt)]

    async def save_publication_message(self, publication_message: PublicationMessage) -> None:
        data = {
            "public_key": publication_message.public_key,
            "cuckoo_filter": packb(publication_message.cuckoo_filter),
            "nym": publication_message.nym,
            "nb_docs": publication_message.num_documents,
            "created_at": datetime.datetime.utcnow()
        }
        stmt = insert(publication_message_table).values(data)
        return await self.database.execute(stmt)


def create_publication_message(row):
    return PublicationMessage(row['nym'], row['public_key'], unpackb(row['cuckoo_filter']), row['nb_docs'])
