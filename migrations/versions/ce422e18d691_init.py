"""init

Revision ID: ce422e18d691
Revises: 
Create Date: 2022-12-06 13:09:06.123534

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ce422e18d691'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('conversation',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('secret_key', sa.LargeBinary(), nullable=False),
    sa.Column('public_key', sa.LargeBinary(), nullable=False),
    sa.Column('other_public_key', sa.LargeBinary(), nullable=False),
    sa.Column('querier', sa.Boolean(), nullable=False),
    sa.Column('query', sa.LargeBinary(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_conversation_other_public_key'), 'conversation', ['other_public_key'], unique=False)
    op.create_index(op.f('ix_conversation_public_key'), 'conversation', ['public_key'], unique=False)
    op.create_table('parameter',
    sa.Column('key', sa.String(length=16), nullable=False),
    sa.Column('value', sa.String(length=36), nullable=False),
    sa.PrimaryKeyConstraint('key')
    )
    op.create_table('peer',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('public_key', sa.LargeBinary(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_peer_public_key'), 'peer', ['public_key'], unique=True)
    op.create_table('publication',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('secret_key', sa.LargeBinary(), nullable=False),
    sa.Column('secret', sa.LargeBinary(), nullable=False),
    sa.Column('nym', sa.String(length=16), nullable=False),
    sa.Column('nb_docs', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_publication_created_at'), 'publication', ['created_at'], unique=False)
    op.create_index(op.f('ix_publication_nym'), 'publication', ['nym'], unique=False)
    op.create_table('publication_message',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('public_key', sa.LargeBinary(), nullable=False),
    sa.Column('cuckoo_filter', sa.LargeBinary(), nullable=False),
    sa.Column('nym', sa.String(length=16), nullable=False),
    sa.Column('nb_docs', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('public_key', sqlite_on_conflict="IGNORE")
    )
    op.create_index(op.f('ix_publication_message_created_at'), 'publication_message', ['created_at'], unique=False)
    op.create_index(op.f('ix_publication_message_nym'), 'publication_message', ['nym'], unique=False)
    op.create_index(op.f('ix_publication_message_public_key'), 'publication_message', ['public_key'], unique=False)
    op.create_table('server_key',
    sa.Column('master_key', sa.LargeBinary(), nullable=False),
    sa.Column('timestamp', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('master_key')
    )
    op.create_table('token',
    sa.Column('secret_key', sa.LargeBinary(), nullable=False),
    sa.Column('token', sa.LargeBinary(), nullable=False),
    sa.Column('timestamp', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('secret_key')
    )
    op.create_table('message',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('address', sa.LargeBinary(), nullable=True),
    sa.Column('timestamp', sa.DateTime(), nullable=False),
    sa.Column('from_key', sa.LargeBinary(), nullable=False),
    sa.Column('payload', sa.LargeBinary(), nullable=True),
    sa.Column('conversation_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['conversation_id'], ['conversation.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_message_from_key'), 'message', ['from_key'], unique=False)
    op.create_table('pigeonhole',
    sa.Column('address', sa.LargeBinary(), nullable=False),
    sa.Column('adr_hex', sa.String(length=8), nullable=False),
    sa.Column('dh_key', sa.LargeBinary(), nullable=False),
    sa.Column('key_for_hash', sa.LargeBinary(), nullable=False),
    sa.Column('message_number', sa.Integer(), nullable=False),
    sa.Column('conversation_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['conversation_id'], ['conversation.id'], ),
    sa.PrimaryKeyConstraint('address')
    )
    op.create_index(op.f('ix_pigeonhole_adr_hex'), 'pigeonhole', ['adr_hex'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_pigeonhole_adr_hex'), table_name='pigeonhole')
    op.drop_table('pigeonhole')
    op.drop_index(op.f('ix_message_from_key'), table_name='message')
    op.drop_table('message')
    op.drop_table('token')
    op.drop_table('server_key')
    op.drop_index(op.f('ix_publication_message_public_key'), table_name='publication_message')
    op.drop_index(op.f('ix_publication_message_nym'), table_name='publication_message')
    op.drop_index(op.f('ix_publication_message_created_at'), table_name='publication_message')
    op.drop_table('publication_message')
    op.drop_index(op.f('ix_publication_nym'), table_name='publication')
    op.drop_index(op.f('ix_publication_created_at'), table_name='publication')
    op.drop_table('publication')
    op.drop_index(op.f('ix_peer_public_key'), table_name='peer')
    op.drop_table('peer')
    op.drop_table('parameter')
    op.drop_index(op.f('ix_conversation_public_key'), table_name='conversation')
    op.drop_index(op.f('ix_conversation_other_public_key'), table_name='conversation')
    op.drop_table('conversation')
    # ### end Alembic commands ###