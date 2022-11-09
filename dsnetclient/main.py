import asyncio
import datetime
import logging
from cmd import Cmd
from enum import Enum, unique
from functools import wraps
from getpass import getpass
from pathlib import Path
from random import expovariate, getrandbits
from typing import List, Optional

import alembic.config
from dsnet.mspsi import Document

from dsnetclient.form_parser import bs_parser
from dsnetclient.query_encoder import LuceneEncoder, MSPSIEncoder, SearchMode

try:
    import readline
except ImportError:
    readline = None

import click
import databases
import dsnet
from dsnet.crypto import get_public_key, gen_key_pair
from dsnet.logger import add_stdout_handler
from elasticsearch import AsyncElasticsearch
from yarl import URL

from dsnetclient import __version__
from dsnetclient.api import DsnetApi, InvalidAuthorizationResponse
from dsnetclient.index import MemoryIndex, Index, LuceneIndex, NamedEntity, NamedEntityCategory
from dsnetclient.message_retriever import AddressMatchMessageRetriever, ProbabilisticCoverMessageRetriever
from dsnetclient.message_sender import DirectMessageSender, QueueMessageSender
from dsnetclient.mutually_exclusive_click import MutuallyExclusiveOption
from dsnetclient.repository import SqlalchemyRepository, Peer


def asynccmd(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwds))
    return wrapper


class Demo(Cmd):
    def __init__(self, server_url: URL, token_url: URL, private_key: str, database_url, keys: List[str], index: Index,
                 search_mode: SearchMode, message_retriever=None, message_sender=None,
                 history_file: Path = None, history_file_size=1000):
        super().__init__()
        self.search_mode = search_mode
        self.database = databases.Database(database_url)
        self.private_key = bytes.fromhex(private_key)
        self.public_key = get_public_key(self.private_key)
        self.repository = SqlalchemyRepository(self.database)
        self.api = DsnetApi(
            server_url,
            token_url,
            self.repository,
            message_retriever=AddressMatchMessageRetriever(server_url, self.repository) if message_retriever is None else message_retriever,
            message_sender=DirectMessageSender(server_url) if message_sender is None else message_sender,
            query_encoder=MSPSIEncoder() if search_mode == SearchMode.DPSI else LuceneEncoder(),
            secret_key=self.private_key,
            index=index
        )
        self._listener = self.api.background_listening()
        add_stdout_handler(level=logging.DEBUG)
        self.prompt = f'ds@{self.public_key[0:4].hex()}> '
        for key_hex in keys:
            key = bytes.fromhex(key_hex)
            if private_key != self.public_key:
                asyncio.get_event_loop().run_until_complete(self.repository.save_peer(Peer(key)))

        if history_file is None:
            history_file = Path.home() / ".dsnet_history"

        self.history_file = history_file.resolve()
        self.histfile_size = history_file_size

    def preloop(self):
        if readline and self.history_file.is_file():
            readline.read_history_file(self.history_file)

    def postloop(self):
        if readline:
            readline.set_history_length(self.histfile_size)
            readline.write_history_file(self.history_file)

    @asynccmd
    async def do_version(self, _line) -> Optional[bool]:
        """
        display client/server version of datashare network
        """
        server_version = await self.api.get_server_version()
        print(f"client {__version__} (core {dsnet.__version__}) with {server_version['message']} (core {server_version['core_version']})")
        return False

    @asynccmd
    async def do_query(self, line: str) -> Optional[bool]:
        """
        send a query to datashare network
        :param query: query to broadcast
        """
        print(f"send query: {line}")
        await self.api.send_query(line.encode())
        return False

    @asynccmd
    async def do_get_tokens(self, _line: str):
        """
        get pretokens from token server and compute Abe blind tokens. They are stored in the local repository.
        """
        username = input("Username: ")
        password = getpass()
        try:
            nb_tokens = await self.api.fetch_pre_tokens(username, password, bs_parser)
            print(f"retrieved {nb_tokens} token{'s' if nb_tokens > 1 else ''}")
        except InvalidAuthorizationResponse:
            print("Invalid username or password!")
        return False

    @asynccmd
    async def do_tokens(self, _line: str):
        """
        show tokens from the local repository.
        """
        tokens = await self.api.show_tokens()
        for i, token in enumerate(tokens):
            print(f"{i+1:02}: [32:64] {token.hex()[32:64]} {len(token)}")
        return False

    @asynccmd
    async def do_queries(self, _line: str) -> Optional[bool]:
        """
        list the queries sent or received (i.e. conversations)
        """
        conversations = await self.api.repository.get_conversations()
        for conversation in conversations:
            query = conversation.query.decode() if conversation.query else ""
            print(f"{conversation.id}: {query} for {conversation.other_public_key.hex()} "
                  f"(sent: {conversation.nb_sent_messages}/recv: {conversation.nb_recv_messages})")
        return False

    @asynccmd
    async def do_phs(self, _line: str) -> Optional[bool]:
        """
        list the waiting pigeon holes
        """
        phs = await self.api.repository.get_pigeonholes()
        for ph in phs:
            print(f"{ph.address.hex() if ph.address else ''}: nb msg ({ph.message_number}) (conversation id={ph.conversation_id})")
        return False

    @asynccmd
    async def do_peers(self, _line: str) -> Optional[bool]:
        """
        list the peers keys
        """
        peers = await self.api.repository.peers()
        for peer in peers:
            print(f"{peer.id}: {peer.public_key.hex()} {'(me)' if self.public_key == peer.public_key else ''}")
        return False

    @asynccmd
    async def do_messages(self, line: str) -> Optional[bool]:
        """
        list the messages related to a conversation
        :param id: conversation id
        """
        conv = await self.api.repository.get_conversation(int(line))
        if conv is not None:
            for msg in conv._messages:
                address = msg.address.hex() if msg.address is not None else 'query'
                print(f"{address} ({msg.timestamp}): {msg.payload} from {msg.from_key.hex()}")
        else:
            print('no such conversation id')
        return False

    @asynccmd
    async def do_message(self, line: str) -> Optional[bool]:
        """
        send a message to a conversation identified with its id
        :param conversation_id
        """
        print(line)
        conv_id, message = line.split(maxsplit=1)
        await self.api.send_message(int(conv_id), message.encode())
        return False

    @asynccmd
    async def do_pk(self, _) -> Optional[bool]:
        """
        displays the user public key
        """
        print(self.public_key.hex())
        return False

    @asynccmd
    async def do_publish(self, _) -> Optional[bool]:
        """
        publish MSPSI index of hashes
        """
        await self.api.send_publication()
        return False

    @asynccmd
    async def do_EOF(self, line):
        """
        type CTRL+D or CTRL+C to exit
        """
        return await self._exit(line)

    async def _exit(self, line: str) -> Optional[bool]:
        await self.api.close()
        print('Bye!')
        return True


@click.group()
@click.pass_context
def cli(ctx, **options):
    # Pass all option to context
    ctx.ensure_object(dict)
    ctx.obj.update(options)


@cli.command()
@click.option('--private-key', prompt='User private key file', help='Private key file')
@click.option('--public-key', prompt='User public key file', help='Public key file')
@click.option('--num-other-public-keys', prompt='Number of other public keys', help='Number of other public keys')
def gen_keys(private_key: str, public_key: str, num_other_public_keys: str):
    sk, pk = gen_key_pair()
    sk_hex = sk.hex()
    pk_hex = pk.hex()

    Path(private_key).write_text(sk_hex)
    with Path(public_key).open("w") as fd:
        fd.write(pk_hex + "\n")

    with Path(public_key).open("a") as fd:
        for _ in range(int(num_other_public_keys)):
            _, pk = gen_key_pair()
            pk_hex = pk.hex()
            fd.write(pk_hex + "\n")


@cli.command()
@click.option('--database-url', prompt='Database file', help='Sqlite url ex: sqlite:///path/to/sqlfile')
def migrate(database_url: str) -> None:
    _migrate(database_url)


def _migrate(database_url: str) -> None:
    args = [
        '--raiseerr',
        '-x', f'dbPath={database_url}',
        'upgrade', 'head'
    ]
    alembic.config.main(argv=args)


@cli.command()
@click.option('--server-url', prompt='Server url', help='The http url where the server can be joined')
@click.option('--token-server-url', prompt='Token server url', help='The http url where the token server can be joined')
@click.option('--private-key', prompt='User private key', help='Private key file (prefix with @)')
@click.option('--database-url', prompt='Database file', help='Sqlite url ex: sqlite:///path/to/sqlfile')
@click.option('--keys', prompt='Others\' key', help='Path to file containing keys (one key per line)')
@click.option('--elasticsearch-url', cls=MutuallyExclusiveOption, help='Elasticsearch url ex: http://elasticsearch:9200',  mutually_exclusive=["entities_file"])
@click.option('--elasticsearch-index', help='Elasticsearch index ex: local-datashare', default='local-datashare')
@click.option('--entities-file', cls=MutuallyExclusiveOption, help='Entities files (one per line)',  mutually_exclusive=["elasticsearch_url"])
@click.option('--cover/--no-cover', help='Hide real messages with a cover.', default=False)
@click.option('--history-file', help="Client's history file", required=False, type=click.Path(), default=(Path.home()/".dsnet_history"))
@click.option('--history-size', help="Client's history size", required=False, default=1000)
@click.option('--search-mode', help='The search mode for the client', type=click.Choice(list(map(lambda x: x.name, SearchMode))), required=False, default='Lucene', callback=lambda ctx, param, value: SearchMode[value])
def shell(server_url, token_server_url, private_key, database_url,
          elasticsearch_url, elasticsearch_index, keys, entities_file, cover, history_file, history_size, search_mode: SearchMode):
    with open(private_key, "r") as f:
        private_key_content = f.read()

    with open(keys, "r") as f:
        keys_list = f.readlines()

    if elasticsearch_url is not None:
        index = LuceneIndex(AsyncElasticsearch(elasticsearch_url), elasticsearch_index)
    else:
        with open(entities_file, "r") as f:
            my_entities = f.readlines()
        entities = [NamedEntity("doc_id", NamedEntityCategory.PERSON, e.strip('\n')) for e in my_entities]
        index = MemoryIndex(entities, [Document("doc_id", datetime.datetime.utcnow())])

    message_sender = None
    message_retriever = None
    if cover:
        message_sender = QueueMessageSender(
            URL(server_url), lambda: expovariate(0.2)
        )
        message_retriever = ProbabilisticCoverMessageRetriever(
            URL(server_url), SqlalchemyRepository(database_url), lambda: bool(getrandbits(1))
        )

    _migrate(database_url)

    Demo(
        URL(server_url),
        URL(token_server_url),
        private_key_content,
        database_url,
        keys_list,
        index,
        search_mode,
        history_file=history_file,
        history_file_size=history_size,
        message_retriever=message_retriever,
        message_sender=message_sender
    ).cmdloop()


if __name__ == '__main__':
    cli()
