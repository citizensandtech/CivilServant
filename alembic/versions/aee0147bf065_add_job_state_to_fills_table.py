"""add job_state to fills table

Revision ID: aee0147bf065
Revises: 3b4dbe799706
Create Date: 2019-01-10 18:03:17.679324

"""

# revision identifiers, used by Alembic.
revision = 'aee0147bf065'
down_revision = '3b4dbe799706'
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
    op.add_column('twitter_fills', sa.Column('job_state', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade_development():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('twitter_fills', 'job_state')
    # ### end Alembic commands ###


def upgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('twitter_fills', sa.Column('job_state', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('twitter_fills', 'job_state')
    # ### end Alembic commands ###


def upgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('twitter_fills', sa.Column('job_state', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('twitter_fills', 'job_state')
    # ### end Alembic commands ###
