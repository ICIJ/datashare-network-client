from sqlalchemy import Table, Column, Integer, LargeBinary, MetaData, ForeignKey, DateTime, Text, Boolean, String

# Database table definitions.
metadata = MetaData()

parameter_table = Table(
    "parameter",
    metadata,
    Column("key", String(16), primary_key=True),
    Column("value", String(36), nullable=False),
)

publication_table = Table(
    "publication",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("secret_key", LargeBinary, unique=False, nullable=False),
    Column("secret", LargeBinary, unique=False, nullable=False),
    Column("nym", String(16), index=True, nullable=False),
    Column("nb_docs", Integer, nullable=False),
    Column("created_at", DateTime, index=True, nullable=False),
)

publication_message_table = Table(
    "publication_message",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("public_key", LargeBinary, unique=True, index=True, nullable=False),
    Column("cuckoo_filter", LargeBinary, index=False, nullable=False),
    Column("nym", String(16), index=True, nullable=False),
    Column("nb_docs", Integer, nullable=False),
    Column("created_at", DateTime, index=True, nullable=False),
)

peer_table = Table(
    "peer",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("public_key", LargeBinary, unique=True, index=True, nullable=False)
)

conversation_table = Table(
    "conversation",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("secret_key", LargeBinary, nullable=False),
    Column("public_key", LargeBinary, index=True, nullable=False),
    Column("other_public_key", LargeBinary, index=True, nullable=False),
    Column('querier', Boolean, nullable=False, default=False),
    Column('query', LargeBinary, nullable=True),
    Column('created_at', DateTime, nullable=False),
)

message_table = Table(
    "message",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("address", LargeBinary, nullable=True),
    Column('timestamp', DateTime, nullable=False),
    Column("from_key", LargeBinary, index=True, nullable=False),
    Column("payload", LargeBinary, nullable=True),
    Column("conversation_id", Integer, ForeignKey('conversation.id'), nullable=False),
)

pigeonhole_table = Table(
    "pigeonhole",
    metadata,
    Column("address", LargeBinary, primary_key=True),
    Column("adr_hex", String(8), nullable=False, index=True),
    Column("dh_key", LargeBinary, nullable=False),
    Column("key_for_hash", LargeBinary, nullable=False),
    Column("message_number", Integer, nullable=False),
    Column("conversation_id", Integer, ForeignKey('conversation.id'), nullable=False)
)

serverkey_table = Table(
    "server_key",
    metadata,
    Column("master_key", LargeBinary, primary_key=True),
    Column('timestamp', DateTime, nullable=False)
)

token_table = Table(
    "token",
    metadata,
    Column("secret_key", LargeBinary, primary_key=True),
    Column("token", LargeBinary, nullable=False),
    Column('timestamp', DateTime, nullable=False)
)
