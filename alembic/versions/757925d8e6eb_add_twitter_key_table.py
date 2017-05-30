"""add twitter key table

Revision ID: 757925d8e6eb
Revises: 16dbded8a5cf
Create Date: 2017-05-12 04:45:06.386576

"""

# revision identifiers, used by Alembic.
revision = '757925d8e6eb'
down_revision = '16dbded8a5cf'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_development():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('twitter_keys',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('screen_name', sa.String(length=64), nullable=True),
    sa.Column('twitter_id', sa.BigInteger(), nullable=True),
    sa.Column('oauth_token_secret', sa.String(length=256), nullable=True),
    sa.Column('oauth_token', sa.String(length=256), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('rate_limit_until', sa.DateTime(), nullable=True),
    sa.Column('rate_limit_remaining', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_twitter_keys_rate_limit_until'), 'twitter_keys', ['rate_limit_until'], unique=False)
    op.create_index(op.f('ix_twitter_keys_screen_name'), 'twitter_keys', ['screen_name'], unique=False)
    op.create_index(op.f('ix_twitter_keys_updated_at'), 'twitter_keys', ['updated_at'], unique=False)
    ### end Alembic commands ###


def downgrade_development():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_twitter_keys_updated_at'), table_name='twitter_keys')
    op.drop_index(op.f('ix_twitter_keys_screen_name'), table_name='twitter_keys')
    op.drop_index(op.f('ix_twitter_keys_rate_limit_until'), table_name='twitter_keys')
    op.drop_table('twitter_keys')
    ### end Alembic commands ###


def upgrade_test():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('twitter_keys',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('screen_name', sa.String(length=64), nullable=True),
    sa.Column('twitter_id', sa.BigInteger(), nullable=True),
    sa.Column('oauth_token_secret', sa.String(length=256), nullable=True),
    sa.Column('oauth_token', sa.String(length=256), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('rate_limit_until', sa.DateTime(), nullable=True),
    sa.Column('rate_limit_remaining', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_twitter_keys_rate_limit_until'), 'twitter_keys', ['rate_limit_until'], unique=False)
    op.create_index(op.f('ix_twitter_keys_screen_name'), 'twitter_keys', ['screen_name'], unique=False)
    op.create_index(op.f('ix_twitter_keys_updated_at'), 'twitter_keys', ['updated_at'], unique=False)
#    op.drop_table('lumen_notices')
#    op.drop_table('twitter_statuses')
#    op.drop_table('twitter_user_snapshots')
#    op.drop_table('twitter_users')
#    op.drop_table('lumen_notice_to_twitter_user')
    ### end Alembic commands ###


def downgrade_test():
    ### commands auto generated by Alembic - please adjust! ###
#    op.create_table('lumen_notice_to_twitter_user',
#    sa.Column('id', mysql.INTEGER(display_width=11), nullable=False),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('notice_id', mysql.BIGINT(display_width=20), autoincrement=False, nullable=True),
#    sa.Column('twitter_username', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('twitter_user_id', mysql.VARCHAR(length=64), nullable=True),
#    sa.Column('CS_account_archived', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('twitter_users',
#    sa.Column('id', mysql.VARCHAR(length=64), nullable=False),
#    sa.Column('screen_name', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('lang', mysql.VARCHAR(length=32), nullable=True),
#    sa.Column('user_state', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.Column('CS_oldest_tweets_archived', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('twitter_user_snapshots',
#    sa.Column('id', mysql.INTEGER(display_width=11), nullable=False),
#    sa.Column('twitter_user_id', mysql.VARCHAR(length=64), nullable=True),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('user_state', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.Column('user_json', mysql.MEDIUMTEXT(), nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('twitter_statuses',
#    sa.Column('id', mysql.BIGINT(display_width=20), nullable=False),
#    sa.Column('user_id', mysql.VARCHAR(length=64), nullable=True),
#    sa.Column('created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('status_data', mysql.MEDIUMTEXT(), nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('lumen_notices',
#    sa.Column('id', mysql.BIGINT(display_width=20), nullable=False),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('date_received', mysql.DATETIME(), nullable=True),
#    sa.Column('sender', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('principal', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('recipient', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('notice_data', mysql.MEDIUMTEXT(), nullable=True),
#    sa.Column('CS_parsed_usernames', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
    op.drop_index(op.f('ix_twitter_keys_updated_at'), table_name='twitter_keys')
    op.drop_index(op.f('ix_twitter_keys_screen_name'), table_name='twitter_keys')
    op.drop_index(op.f('ix_twitter_keys_rate_limit_until'), table_name='twitter_keys')
    op.drop_table('twitter_keys')
    ### end Alembic commands ###


def upgrade_production():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('twitter_keys',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('screen_name', sa.String(length=64), nullable=True),
    sa.Column('twitter_id', sa.BigInteger(), nullable=True),
    sa.Column('oauth_token_secret', sa.String(length=256), nullable=True),
    sa.Column('oauth_token', sa.String(length=256), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('rate_limit_until', sa.DateTime(), nullable=True),
    sa.Column('rate_limit_remaining', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_twitter_keys_rate_limit_until'), 'twitter_keys', ['rate_limit_until'], unique=False)
    op.create_index(op.f('ix_twitter_keys_screen_name'), 'twitter_keys', ['screen_name'], unique=False)
    op.create_index(op.f('ix_twitter_keys_updated_at'), 'twitter_keys', ['updated_at'], unique=False)
#    op.drop_table('lumen_notices')
#    op.drop_table('twitter_statuses')
#    op.drop_table('twitter_user_snapshots')
#    op.drop_table('twitter_users')
#    op.drop_table('lumen_notice_to_twitter_user')
    ### end Alembic commands ###


def downgrade_production():
    ### commands auto generated by Alembic - please adjust! ###
#    op.create_table('lumen_notice_to_twitter_user',
#    sa.Column('id', mysql.INTEGER(display_width=11), nullable=False),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('notice_id', mysql.BIGINT(display_width=20), autoincrement=False, nullable=True),
#    sa.Column('twitter_username', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('twitter_user_id', mysql.VARCHAR(length=64), nullable=True),
#    sa.Column('CS_account_archived', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('twitter_users',
#    sa.Column('id', mysql.VARCHAR(length=64), nullable=False),
#    sa.Column('screen_name', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('lang', mysql.VARCHAR(length=32), nullable=True),
#    sa.Column('user_state', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.Column('CS_oldest_tweets_archived', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('twitter_user_snapshots',
#    sa.Column('id', mysql.INTEGER(display_width=11), nullable=False),
#    sa.Column('twitter_user_id', mysql.VARCHAR(length=64), nullable=True),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('user_state', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.Column('user_json', mysql.MEDIUMTEXT(), nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('twitter_statuses',
#    sa.Column('id', mysql.BIGINT(display_width=20), nullable=False),
#    sa.Column('user_id', mysql.VARCHAR(length=64), nullable=True),
#    sa.Column('created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('status_data', mysql.MEDIUMTEXT(), nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
#    op.create_table('lumen_notices',
#    sa.Column('id', mysql.BIGINT(display_width=20), nullable=False),
#    sa.Column('record_created_at', mysql.DATETIME(), nullable=True),
#    sa.Column('date_received', mysql.DATETIME(), nullable=True),
#    sa.Column('sender', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('principal', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('recipient', mysql.VARCHAR(length=256), nullable=True),
#    sa.Column('notice_data', mysql.MEDIUMTEXT(), nullable=True),
#    sa.Column('CS_parsed_usernames', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True),
#    sa.PrimaryKeyConstraint('id'),
#    mysql_default_charset='latin1',
#    mysql_engine='InnoDB'
#    )
    op.drop_index(op.f('ix_twitter_keys_updated_at'), table_name='twitter_keys')
    op.drop_index(op.f('ix_twitter_keys_screen_name'), table_name='twitter_keys')
    op.drop_index(op.f('ix_twitter_keys_rate_limit_until'), table_name='twitter_keys')
    op.drop_table('twitter_keys')
    ### end Alembic commands ###
