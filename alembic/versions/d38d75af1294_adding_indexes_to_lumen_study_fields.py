"""adding indexes to lumen study fields

Revision ID: d38d75af1294
Revises: 59dd3251b28b
Create Date: 2018-02-18 13:35:59.527624

"""

# revision identifiers, used by Alembic.
revision = 'd38d75af1294'
down_revision = '59dd3251b28b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_development():
    op.create_index(op.f('ix_lumen_notices_created_at_index'), 'lumen_notices', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_lumen_notice_twitter_user_created_at_index'), 'lumen_notice_to_twitter_user', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_twitter_users_created_at_index'), 'twitter_users', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_twitter_statuses_created_at_index'), 'twitter_statuses', ['record_created_at'], unique=False)

def downgrade_development():
    op.drop_index(op.f('ix_lumen_notices_created_at_index'), table_name='lumen_notices')
    op.drop_index(op.f('ix_lumen_notice_twitter_user_created_at_index'), table_name='lumen_notice_to_twitter_user')
    op.drop_index(op.f('ix_twitter_users_created_at_index'), table_name='twitter_users')
    op.drop_index(op.f('ix_twitter_statuses_created_at_index'), table_name='twitter_statuses')

def upgrade_test():
    op.create_index(op.f('ix_lumen_notices_created_at_index'), 'lumen_notices', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_lumen_notice_twitter_user_created_at_index'), 'lumen_notice_to_twitter_user', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_twitter_users_created_at_index'), 'twitter_users', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_twitter_statuses_created_at_index'), 'twitter_statuses', ['record_created_at'], unique=False)

def downgrade_test():
    op.drop_index(op.f('ix_lumen_notices_created_at_index'), table_name='lumen_notices')
    op.drop_index(op.f('ix_lumen_notice_twitter_user_created_at_index'), table_name='lumen_notice_to_twitter_user')
    op.drop_index(op.f('ix_twitter_users_created_at_index'), table_name='twitter_users')
    op.drop_index(op.f('ix_twitter_statuses_created_at_index'), table_name='twitter_statuses')

def upgrade_production():
    op.create_index(op.f('ix_lumen_notices_created_at_index'), 'lumen_notices', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_lumen_notice_twitter_user_created_at_index'), 'lumen_notice_to_twitter_user', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_twitter_users_created_at_index'), 'twitter_users', ['record_created_at'], unique=False)
    op.create_index(op.f('ix_twitter_statuses_created_at_index'), 'twitter_statuses', ['record_created_at'], unique=False)

def downgrade_production():
    op.drop_index(op.f('ix_lumen_notices_created_at_index'), table_name='lumen_notices')
    op.drop_index(op.f('ix_lumen_notice_twitter_user_created_at_index'), table_name='lumen_notice_to_twitter_user')
    op.drop_index(op.f('ix_twitter_users_created_at_index'), table_name='twitter_users')
    op.drop_index(op.f('ix_twitter_statuses_created_at_index'), table_name='twitter_statuses')
