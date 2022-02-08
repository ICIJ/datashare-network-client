import asyncio
import cmd
from pathlib import Path
from typing import List, Set, Optional

import click
import databases
from dsnet.crypto import get_public_key, gen_key_pair
from dsnet.message import Message
from yarl import URL

from dsnetclient.api import DsnetApi
from dsnetclient.index import MemoryIndex
from dsnetclient.repository import SqlalchemyRepository, Peer


class Demo(cmd.Cmd):
    def __init__(self, server_url: URL, private_key: str, database_url, keys: List[str], my_entities: Set[str]):
        super().__init__()
        self.database = databases.Database(database_url)
        self.private_key = bytes.fromhex(private_key)
        self.public_key = get_public_key(self.private_key)
        self.repository = SqlalchemyRepository(self.database)
        self.api = DsnetApi(server_url, self.repository, private_key=self.private_key, index=MemoryIndex(my_entities))
        self._listener = self.api.background_listening(self.display_message)
        self.my_entities = my_entities
        self.keys: List[bytes] = list()
        self.prompt = 'DS> '
        for key_hex in keys:
            key = bytes.fromhex(key_hex)
            if private_key != self.public_key:
                self.keys.append(key)
                asyncio.get_event_loop().run_until_complete(self.repository.save_peer(Peer(key)))

    async def display_message(self, msg: Message) -> None:
        print(f'received msg type : {msg.type()}')
        await self.api.websocket_callback(msg)

    def do_server_version(self, _line) -> Optional[bool]:
        print(asyncio.get_event_loop().run_until_complete(self.api.get_server_version()))
        return False

    def do_query(self, line: str) -> Optional[bool]:
        print(f"send query: {line}")
        asyncio.get_event_loop().run_until_complete(self.api.send_query(line.encode()))
        return False

    def do_list_queries(self, _line: str) -> Optional[bool]:
        conversations = asyncio.get_event_loop().run_until_complete(self.api.repository.get_conversations())
        for conversation in conversations:
            query = conversation.query.decode() if conversation.query else ""
            print(f"{conversation.id}. {query}")
        return False

    def do_messages(self, line: str) -> Optional[bool]:
        conversation = asyncio.get_event_loop().run_until_complete(self.api.repository.get_conversations_filter_by(id=int(line)))
        for msg in conversation._messages:
            print(f"{msg.address.hex()} ({msg.timestamp}): {msg.payload}")
        return False

    def do_bye(self, line):
        return self._exit(line)

    def do_EOF(self, line):
        return self._exit(line)

    def _exit(self, line: str) -> Optional[bool]:
        self._listener.cancel()
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
@click.option('--entities-file', prompt='Entities file', help='Entities files (one per line)')
def shell(server_url, private_key, database_url, keys, entities_file):
    with open(private_key, "r") as f:
        private_key_content = f.read()

    with open(keys, "r") as f:
        keys_list = f.readlines()

    with open(entities_file, "r") as f:
        my_entities = f.readlines()

    Demo(URL(server_url), private_key_content, database_url, keys_list, set(my_entities)).cmdloop()


if __name__ == '__main__':
    cli()