from sqlalchemy import Table, Column, Integer, LargeBinary, MetaData, ForeignKey

# Database table definitions.
metadata = MetaData()

conversation_table = Table(
    "conversation",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("private_key", LargeBinary, nullable=False),
    Column("public_key", LargeBinary, index=True, nullable=False),
)

pigeonhole_table = Table(
    "pigeonhole",
    metadata,
    Column("address", LargeBinary, primary_key=True),
    Column("dh_key", LargeBinary),
    Column("public_key", LargeBinary),
    Column("message_number", Integer),
    Column("conversation_id", Integer, ForeignKey('conversation.id')),
)
