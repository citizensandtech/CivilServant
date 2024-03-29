"""Increase the max size of access_token on praw_keys

Revision ID: 8d2a661ad4b3
Revises: 365caa1cbd80
Create Date: 2023-05-16 19:39:53.125209

"""

# revision identifiers, used by Alembic.
revision = '8d2a661ad4b3'
down_revision = '365caa1cbd80'
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
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('praw_keys', 'access_token',
               existing_type=mysql.VARCHAR(length=256),
               type_=sa.String(length=2048),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade_development():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('praw_keys', 'access_token',
               existing_type=sa.String(length=2048),
               type_=mysql.VARCHAR(length=256),
               existing_nullable=True)
    # ### end Alembic commands ###


def upgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('praw_keys', 'access_token',
               existing_type=mysql.VARCHAR(length=256),
               type_=sa.String(length=2048),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('praw_keys', 'access_token',
               existing_type=sa.String(length=2048),
               type_=mysql.VARCHAR(length=256),
               existing_nullable=True)
    # ### end Alembic commands ###


def upgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('praw_keys', 'access_token',
               existing_type=mysql.VARCHAR(length=256),
               type_=sa.String(length=2048),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('praw_keys', 'access_token',
               existing_type=sa.String(length=2048),
               type_=mysql.VARCHAR(length=256),
               existing_nullable=True)
    # ### end Alembic commands ###

