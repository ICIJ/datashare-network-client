from sqlalchemy import Table, Column, Integer, LargeBinary, MetaData, ForeignKey, DateTime, Text, Boolean

# Database table definitions.
metadata = MetaData()


peer_table = Table(
    "peer",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("public_key", LargeBinary, index=True, nullable=False)
)


conversation_table = Table(
    "conversation",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("private_key", LargeBinary, nullable=False),
    Column("public_key", LargeBinary, index=True, nullable=False),
    Column("other_public_key", LargeBinary, index=True, nullable=False),
    Column('nb_sent_messages', Integer, nullable=False),
    Column('nb_recv_messages', Integer, nullable=False),
    Column('querier', Boolean, nullable=False, default=False),
    Column('query', LargeBinary, nullable=True),
    Column('created_at', DateTime, nullable=False),
)

message_table = Table(
    "message",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("address", LargeBinary, nullable=False),
    Column('timestamp', DateTime, nullable=False),
    Column('message_number', Integer, nullable=False),
    Column("from_key", LargeBinary, index=True, nullable=False),
    Column("payload", LargeBinary, nullable=False),
    Column("conversation_id", Integer, ForeignKey('conversation.id'), nullable=False),
)

pigeonhole_table = Table(
    "pigeonhole",
    metadata,
    Column("address", LargeBinary, primary_key=True),
    Column("dh_key", LargeBinary, nullable=False),
    Column("public_key", LargeBinary, nullable=False),
    Column("message_number", Integer, nullable=False),
    Column("conversation_id", Integer, ForeignKey('conversation.id'), nullable=False),
)
