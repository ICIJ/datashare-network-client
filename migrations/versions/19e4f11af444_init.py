"""init

Revision ID: 19e4f11af444
Revises: 
Create Date: 2022-01-31 14:31:50.526182

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '19e4f11af444'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('conversation',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('private_key', sa.LargeBinary(), nullable=False),
    sa.Column('public_key', sa.LargeBinary(), nullable=False),
    sa.Column('other_public_key', sa.LargeBinary(), nullable=False),
    sa.Column('querier', sa.Boolean(), nullable=False),
    sa.Column('query', sa.LargeBinary(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_conversation_other_public_key'), 'conversation', ['other_public_key'], unique=False)
    op.create_index(op.f('ix_conversation_public_key'), 'conversation', ['public_key'], unique=False)
    op.create_table('peer',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('public_key', sa.LargeBinary(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_peer_public_key'), 'peer', ['public_key'], unique=False)
    op.create_table('message',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('address', sa.LargeBinary(), nullable=False),
    sa.Column('timestamp', sa.DateTime(), nullable=False),
    sa.Column('from_key', sa.LargeBinary(), nullable=False),
    sa.Column('payload', sa.LargeBinary(), nullable=False),
    sa.Column('conversation_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['conversation_id'], ['conversation.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_message_from_key'), 'message', ['from_key'], unique=False)
    op.create_table('pigeonhole',
    sa.Column('address', sa.LargeBinary(), nullable=False),
    sa.Column('adr_hex', sa.String(8), nullable=False, index=True),
    sa.Column('dh_key', sa.LargeBinary(), nullable=False),
    sa.Column('public_key', sa.LargeBinary(), nullable=False),
    sa.Column('message_number', sa.Integer(), nullable=False),
    sa.Column('conversation_id', sa.Integer(), nullable=False),
    sa.Column('peer_key', sa.Integer()),
    sa.ForeignKeyConstraint(['conversation_id'], ['conversation.id'], ),
    sa.PrimaryKeyConstraint('address')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('pigeonhole')
    op.drop_index(op.f('ix_message_from_key'), table_name='message')
    op.drop_table('message')
    op.drop_index(op.f('ix_peer_public_key'), table_name='peer')
    op.drop_table('peer')
    op.drop_index(op.f('ix_conversation_public_key'), table_name='conversation')
    op.drop_index(op.f('ix_conversation_other_public_key'), table_name='conversation')
    op.drop_table('conversation')
    # ### end Alembic commands ###
