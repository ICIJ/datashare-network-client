import argparse
import asyncio
import logging
import re
import sys
from getpass import getpass
from pathlib import Path
from random import expovariate, getrandbits
from typing import List

import alembic
from aioconsole import AsynchronousCli, ainput
from aiohttp import ClientSession
from dsnet.core import QueryType

from dsnetclient.form_parser import bs_parser

import click
import databases
import dsnet
from dsnet.crypto import get_public_key, gen_key_pair
from dsnet.logger import add_stdout_handler
from elasticsearch import AsyncElasticsearch
from yarl import URL

from dsnetclient import __version__
from dsnetclient.api import DsnetApi, InvalidAuthorizationResponse
from dsnetclient.index import Index, LuceneIndex, MspsiIndex
from dsnetclient.message_retriever import AddressMatchMessageRetriever, ProbabilisticCoverMessageRetriever
from dsnetclient.message_sender import DirectMessageSender, QueueMessageSender
from dsnetclient.mutually_exclusive_click import MutuallyExclusiveOption
from dsnetclient.repository import SqlalchemyRepository, Peer, Repository

PARAM_EXTRACTOR = re.compile(r':param ([a-z_]+)')


class Demo(AsynchronousCli):
    def __init__(self, server_url: URL, token_url: URL, private_key: str, repository: Repository, keys: List[str], index: Index,
                 query_type: QueryType, message_retriever, message_sender, loop):
        super().__init__({method.replace('do_', ''): (getattr(self, method), get_arg_parser(self, method))
                          for method in dir(self) if method.startswith('do_')}, prog='datashare network', loop=loop)
        self.private_key = bytes.fromhex(private_key)
        self.public_key = get_public_key(self.private_key)
        self.repository = repository
        self.api = DsnetApi(
            server_url,
            token_url,
            self.repository,
            message_retriever=message_retriever,
            message_sender=message_sender,
            query_type=query_type,
            secret_key=self.private_key,
            index=index
        )
        self._listener = self.api.background_listening(loop=self.loop)  
        add_stdout_handler(level=logging.DEBUG)
        sys.ps1 = f'ds@{self.public_key[0:4].hex()}> '
        for key_hex in keys:
            key = bytes.fromhex(key_hex)
            if private_key != self.public_key:
                loop.run_until_complete(self.repository.save_peer(Peer(key)))


    async def do_version(self, _reader, _writer) -> str:
        """
        display client/server version of datashare network
        """
        server_version = await self.api.get_server_version()
        return f"client {__version__} (core {dsnet.__version__}) with {server_version['message']} (core {server_version['core_version']})"

    async def do_query(self, _reader, _writer, query) -> None:
        """
        send a query to datashare network
        :param query: query to broadcast
        """
        print(f"send query: {query}")
        await self.api.send_query(query.encode())

    async def do_get_tokens(self, _reader, _writer) -> str:
        """
        get pretokens from token server and compute Abe blind tokens. They are stored in the local repository.
        """
        username = await ainput("Username: ")
        password = getpass()
        try:
            nb_tokens = await self.api.fetch_pre_tokens(username, password, bs_parser)
            return f"retrieved {nb_tokens} token{'s' if nb_tokens > 1 else ''}"
        except InvalidAuthorizationResponse:
            return "Invalid username or password!"

    async def do_tokens(self, _reader, _writer) -> str:
        """
        show tokens from the local repository.
        """
        tokens = await self.api.show_tokens()
        return '\n'.join([f"{i+1:02}: [32:64] {token.hex()[32:64]} {len(token)}" for i, token in enumerate(tokens)])

    async def do_queries(self, _reader, _writer) -> str:
        """
        list the queries sent or received (i.e. conversations)
        """
        ret = list()
        conversations = await self.api.repository.get_conversations()
        for conversation in conversations:
            query = conversation.query.decode() if conversation.query else ""
            ret.append(f"{conversation.id}: {query} for {conversation.other_public_key.hex()} "
                       f"(sent: {conversation.nb_sent_messages}/recv: {conversation.nb_recv_messages})")
        return '\n'.join(ret)

    async def do_phs(self, _reader, _writer) -> str:
        """
        list the waiting pigeon holes
        """
        phs = await self.api.repository.get_pigeonholes()
        return '\n'.join([f"{ph.address.hex() if ph.address else ''}: nb msg ({ph.message_number}) (conversation id={ph.conversation_id})" for ph in phs])

    async def do_peers(self, _reader, _writer) -> str:
        """
        list the peers keys
        """
        peers = await self.api.repository.peers()
        return '\n'.join([f"{peer.id}: {peer.public_key.hex()} {'(me)' if self.public_key == peer.public_key else ''}" for peer in peers])

    async def do_messages(self, _reader, _writer, id) -> str:
        """
        list the messages related to a conversation
        :param id: conversation id
        """
        conv = await self.api.repository.get_conversation(int(id))
        ret = list()
        if conv is not None:
            for msg in conv._messages:
                address = msg.address.hex() if msg.address is not None else 'query'
                ret.append(f"{address} ({msg.timestamp}): {msg.payload} from {msg.from_key.hex()}")
        else:
            return 'no such conversation id'
        return '\n'.join(ret)

    async def do_message(self, _reader, _writer, conversation_id, message) -> None:
        """
        send a message to a conversation identified with its id
        :param conversation_id
        :param message
        """
        await self.api.send_message(int(conversation_id), message.encode())

    async def do_publication_messages(self, _reader, _writer) -> str:
        """
        list all received publication messages
        """
        publication_messages = await self.repository.get_publication_messages()
        return '\n'.join(f'pkey: {pub_msg.public_key.hex()} number of docs: {pub_msg.num_documents}' for pub_msg in publication_messages)

    async def do_pk(self, _reader, _writer) -> str:
        """
        displays the user public key
        """
        return self.public_key.hex()

    async def do_publish(self, _reader, _writer) -> None:
        """
        publish MSPSI index of hashes
        """
        await self.api.send_publication()

    async def _exit(self, _reader, _writer) -> str:
        await self.api.close()
        return 'Bye!'


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


def get_arg_parser(demo, method):
    method_doc = getattr(demo, method).__doc__
    args = extract_arg_from_docstring(method_doc)
    parser = argparse.ArgumentParser(description=method_doc)
    for arg in args:
        parser.add_argument(arg)
    return parser


def extract_arg_from_docstring(docstring: str) -> List[str]:
    return PARAM_EXTRACTOR.findall(docstring)


async def get_server_config(server_url: URL) -> dict:
    async with ClientSession() as session:
        async with session.get(server_url) as resp:
            return await resp.json()


@cli.command()
@click.option('--server-url', prompt='Server url', help='The http url where the server can be joined')
@click.option('--token-server-url', prompt='Token server url', help='The http url where the token server can be joined')
@click.option('--private-key', prompt='User private key', help='Private key file (prefix with @)')
@click.option('--database-url', prompt='Database file', help='Sqlite url ex: sqlite:///path/to/sqlfile')
@click.option('--keys', prompt='Others\' key', help='Path to file containing keys (one key per line)')
@click.option('--elasticsearch-url', cls=MutuallyExclusiveOption, help='Elasticsearch url ex: http://elasticsearch:9200',  mutually_exclusive=["entities_file"])
@click.option('--elasticsearch-index', help='Elasticsearch index ex: local-datashare', default='local-datashare')
@click.option('--cover/--no-cover', help='Hide real messages with a cover.', default=False)
def shell(server_url, token_server_url, private_key, database_url, elasticsearch_url, elasticsearch_index, keys, cover):
    with open(private_key, "r") as f:
        private_key_content = f.read()

    with open(keys, "r") as f:
        keys_list = f.readlines()

    repository = SqlalchemyRepository(databases.Database(database_url))

    if cover:
        message_sender = QueueMessageSender(
            URL(server_url), lambda: expovariate(0.2)
        )
        message_retriever = ProbabilisticCoverMessageRetriever(
            URL(server_url), repository, lambda: bool(getrandbits(1))
        )
    else:
        message_sender = DirectMessageSender(server_url)
        message_retriever = AddressMatchMessageRetriever(server_url, repository)

    loop = asyncio.new_event_loop()
    config = loop.run_until_complete(get_server_config(URL(server_url)))
    query_type = QueryType(config['query_type'])

    index = LuceneIndex(AsyncElasticsearch(elasticsearch_url), elasticsearch_index)
    if query_type == QueryType.DPSI:
        index = MspsiIndex(repository, index)

    demo = Demo(
        URL(server_url),
        URL(token_server_url),
        private_key_content,
        repository,
        keys_list,
        index,
        query_type,
        message_retriever=message_retriever,
        message_sender=message_sender,
        loop=loop
    )
    loop.run_until_complete(demo.interact())


if __name__ == '__main__':
    cli()
