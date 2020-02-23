"""add resource locks table

Revision ID: 3cb15662f688
Revises: 370964c4a364
Create Date: 2020-02-22 13:52:52.211013

"""

# revision identifiers, used by Alembic.
revision = '3cb15662f688'
down_revision = '370964c4a364'
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
    op.create_table('resource_locks',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('resource', sa.String(length=256), nullable=False),
    sa.Column('experiment_id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('resource', 'experiment_id')
    )
    op.create_index(op.f('ix_resource_locks_created_at'), 'resource_locks', ['created_at'], unique=False)
    op.create_index(op.f('ix_resource_locks_experiment_id'), 'resource_locks', ['experiment_id'], unique=False)
    op.create_index(op.f('ix_resource_locks_resource'), 'resource_locks', ['resource'], unique=False)
    # ### end Alembic commands ###


def downgrade_development():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_resource_locks_resource'), table_name='resource_locks')
    op.drop_index(op.f('ix_resource_locks_experiment_id'), table_name='resource_locks')
    op.drop_index(op.f('ix_resource_locks_created_at'), table_name='resource_locks')
    op.drop_table('resource_locks')
    # ### end Alembic commands ###


def upgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('resource_locks',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('resource', sa.String(length=256), nullable=False),
    sa.Column('experiment_id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('resource', 'experiment_id')
    )
    op.create_index(op.f('ix_resource_locks_created_at'), 'resource_locks', ['created_at'], unique=False)
    op.create_index(op.f('ix_resource_locks_experiment_id'), 'resource_locks', ['experiment_id'], unique=False)
    op.create_index(op.f('ix_resource_locks_resource'), 'resource_locks', ['resource'], unique=False)
    # ### end Alembic commands ###


def downgrade_test():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_resource_locks_resource'), table_name='resource_locks')
    op.drop_index(op.f('ix_resource_locks_experiment_id'), table_name='resource_locks')
    op.drop_index(op.f('ix_resource_locks_created_at'), table_name='resource_locks')
    op.drop_table('resource_locks')
    # ### end Alembic commands ###


def upgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('resource_locks',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('resource', sa.String(length=256), nullable=False),
    sa.Column('experiment_id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('resource', 'experiment_id')
    )
    op.create_index(op.f('ix_resource_locks_created_at'), 'resource_locks', ['created_at'], unique=False)
    op.create_index(op.f('ix_resource_locks_experiment_id'), 'resource_locks', ['experiment_id'], unique=False)
    op.create_index(op.f('ix_resource_locks_resource'), 'resource_locks', ['resource'], unique=False)
    # ### end Alembic commands ###


def downgrade_production():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_resource_locks_resource'), table_name='resource_locks')
    op.drop_index(op.f('ix_resource_locks_experiment_id'), table_name='resource_locks')
    op.drop_index(op.f('ix_resource_locks_created_at'), table_name='resource_locks')
    op.drop_table('resource_locks')
    # ### end Alembic commands ###

