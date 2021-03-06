"""add is_utc_to_pages

Revision ID: 16dbded8a5cf
Revises: a0f4fda7588f
Create Date: 2016-12-14 23:46:54.211173

"""

# revision identifiers, used by Alembic.
revision = '16dbded8a5cf'
down_revision = 'a0f4fda7588f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_development():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('front_pages', sa.Column('is_utc', sa.Boolean(), nullable=True))
    op.add_column('subreddit_pages', sa.Column('is_utc', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade_development():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('subreddit_pages', 'is_utc')
    op.drop_column('front_pages', 'is_utc')
    # ### end Alembic commands ###


def upgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('front_pages', sa.Column('is_utc', sa.Boolean(), nullable=True))
    op.add_column('subreddit_pages', sa.Column('is_utc', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('subreddit_pages', 'is_utc')
    op.drop_column('front_pages', 'is_utc')
    # ### end Alembic commands ###


def upgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('front_pages', sa.Column('is_utc', sa.Boolean(), nullable=True))
    op.add_column('subreddit_pages', sa.Column('is_utc', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('subreddit_pages', 'is_utc')
    op.drop_column('front_pages', 'is_utc')
    # ### end Alembic commands ###

