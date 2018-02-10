"""add created_utc index to mod_actions

Revision ID: df498825ec28
Revises: e566e8e209d1
Create Date: 2018-02-10 14:53:46.146167

"""

# revision identifiers, used by Alembic.
revision = 'df498825ec28'
down_revision = 'e566e8e209d1'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()

def upgrade_development():
    op.create_index(op.f('ix_mod_actions_created_utc_index'), 'mod_actions', ['created_utc'], unique=False)

def downgrade_development():
    op.drop_index(op.f('ix_mod_actions_created_utc_index'), table_name='mod_actions')

def upgrade_test():
    op.create_index(op.f('ix_mod_actions_created_utc_index'), 'mod_actions', ['created_utc'], unique=False)

def downgrade_test():
    op.drop_index(op.f('ix_mod_actions_created_utc_index'), table_name='mod_actions')

def upgrade_production():
    op.create_index(op.f('ix_mod_actions_created_utc_index'), 'mod_actions', ['created_utc'], unique=False)

def downgrade_production():
    op.drop_index(op.f('ix_mod_actions_created_utc_index'), table_name='mod_actions')

