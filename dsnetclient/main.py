import asyncio
import base64
import logging
from pathlib import Path
from typing import List, Set, Optional

import click
import databases
import dsnet
from authlib.integrations.httpx_client import AsyncOAuth2Client
from dsnet.crypto import get_public_key, gen_key_pair
from dsnet.logger import add_stdout_handler
from elasticsearch import AsyncElasticsearch
from yarl import URL

from dsnetclient import __version__
from dsnetclient.api import DsnetApi
from dsnetclient.async_cmd import AsyncCmd
from dsnetclient.index import MemoryIndex, Index, LuceneIndex
from dsnetclient.mutually_exclusive_click import MutuallyExclusiveOption
from dsnetclient.repository import SqlalchemyRepository, Peer


class Demo(AsyncCmd):
    def __init__(self, server_url: URL, private_key: str, database_url, keys: List[str], index: Index, oauth_client: AsyncOAuth2Client):
        super().__init__()
        self.database = databases.Database(database_url)
        self.private_key = bytes.fromhex(private_key)
        self.public_key = get_public_key(self.private_key)
        self.repository = SqlalchemyRepository(self.database)
        self.api = DsnetApi(
            server_url,
            self.repository,
            secret_key=self.private_key,
            index=index,
            oauth_client=oauth_client
        )
        self._listener = self.api.background_listening()
        add_stdout_handler(level=logging.DEBUG)
        self.prompt = f'ds@{self.public_key[0:4].hex()}> '
        for key_hex in keys:
            key = bytes.fromhex(key_hex)
            if private_key != self.public_key:
                asyncio.get_event_loop().run_until_complete(self.repository.save_peer(Peer(key)))

    async def do_version(self, _line) -> Optional[bool]:
        """
        display client/server version of datashare network
        """
        server_version = await self.api.get_server_version()
        print(f"client {__version__} (core {dsnet.__version__}) with {server_version['message']} (core {server_version['core_version']})")
        return False

    async def do_query(self, line: str) -> Optional[bool]:
        """
        send a query to datashare network
        :param query: query to broadcast
        """
        print(f"send query: {line}")
        await self.api.send_query(line.encode())
        return False

    async def do_start_auth(self, line: str) -> Optional[bool]:
        """
        create OAuth2 authentication url and prints it to the console.
        """
        url, state = self.api.start_auth(str(self.api.oauth_client.base_url.join('/oauth/authorize')))
        print(f"copy this url ({url}) in your browser and authenticate. "
              f"Then call end_auth command with the resulting url")
        return False

    async def do_end_auth(self, line: str) -> Optional[bool]:
        """
        Finish authentication with your identity provider.
        """
        success = await self.api.end_auth("/oauth/token", line)
        if success:
            print("You successfully authenticated, you can now order tokens with 'tokens'")
        else:
            print("Authentication failure! Please restart authentication process.")
        return False

    async def do_get_tokens(self, _line: str):
        """
        get pretokens from token server and compute Abe blind tokens. They are stored in the local repository.
        """
        nb_tokens = await self.api.fetch_pre_tokens()
        print(f"retrieved {nb_tokens} token{'s' if nb_tokens > 1 else ''}")
        return False

    async def do_tokens(self, _line: str):
        """
        show tokens from the local repository.
        """
        tokens = await self.api.show_tokens()
        for i, token in enumerate(tokens):
            print(f"{i+1:02}: [32:64] {token.hex()[32:64]} {len(token)}")
        return False

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

    async def do_phs(self, _line: str) -> Optional[bool]:
        """
        list the waiting pigeon holes
        """
        phs = await self.api.repository.get_pigeonholes()
        for ph in phs:
            print(f"{ph.address.hex() if ph.address else ''}: peer {ph.peer_key.hex() if ph.peer_key else '?'} nb msg ({ph.message_number}) (conversation id={ph.conversation_id})")
        return False

    async def do_peers(self, _line: str) -> Optional[bool]:
        """
        list the peers keys
        """
        peers = await self.api.repository.peers()
        for peer in peers:
            print(f"{peer.id}: {peer.public_key.hex()} {'(me)' if self.public_key == peer.public_key else ''}")
        return False

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

    async def do_message(self, line: str) -> Optional[bool]:
        """
        send a message to a conversation identified with its id
        :param conversation_id
        """
        print(line)
        conv_id, message = line.split(maxsplit=1)
        await self.api.send_message(int(conv_id), message.encode())
        return False

    async def do_pk(self, _) -> Optional[bool]:
        """
        displays the user public key
        """
        print(self.public_key.hex())
        return False

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
@click.option('--server-url', prompt='Server url', help='The http url where the server can be joined')
@click.option('--private-key', prompt='User private key', help='Private key file (prefix with @)')
@click.option('--database-url', prompt='Database file', help='Sqlite url ex: sqlite:///path/to/sqlfile')
@click.option('--keys', prompt='Others\' key', help='Path to file containing keys (one key per line)')
@click.option('--elasticsearch-url', cls=MutuallyExclusiveOption, help='Elasticsearch url ex: http://elasticsearch:9200',  mutually_exclusive=["entities_file"])
@click.option('--entities-file', cls=MutuallyExclusiveOption, help='Entities files (one per line)',  mutually_exclusive=["elasticsearch_url"])
@click.option('--oauth-client-id', prompt='Client ID', help='The client ID to authenticate to the identity server.')
@click.option('--oauth-client-secret', prompt='Client secret', help='The client secret to authenticate to the identity server.')
@click.option('--oauth-base-url', prompt='OAuth server base URL', help='The base URL of the identity server.')
def shell(server_url, private_key, database_url, elasticsearch_url, keys, entities_file, oauth_client_id, oauth_client_secret, oauth_base_url):
    with open(private_key, "r") as f:
        private_key_content = f.read()

    with open(keys, "r") as f:
        keys_list = f.readlines()

    if elasticsearch_url is not None:
        index = LuceneIndex(AsyncElasticsearch(elasticsearch_url))
    else:
        with open(entities_file, "r") as f:
            my_entities = f.readlines()
        my_entities = [e.strip('\n') for e in my_entities]
        index = MemoryIndex(set(my_entities))

    # id: '3gfTYQImmGF1hbie72AmmAqiRvmlwU-mRl7-N8QHH2I'
    # secret: 'L29aozJYxn_xauWlpGw2SFJ2VcBphuHOhErPF45kYkg'
    oauth_client = AsyncOAuth2Client(
        oauth_client_id,
        oauth_client_secret,
        redirect_uri="http://localhost:8080/auth/xemx/callback",
        base_url=oauth_base_url
    )

    asyncio.get_event_loop().run_until_complete(Demo(URL(server_url), private_key_content, database_url, keys_list, index, oauth_client).async_cmdloop())


if __name__ == '__main__':
    cli()