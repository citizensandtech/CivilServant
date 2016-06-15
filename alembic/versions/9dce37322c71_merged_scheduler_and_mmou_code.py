"""merged scheduler and mmou code

Revision ID: 9dce37322c71
Revises: 960085fce39c, edc5377c32c7
Create Date: 2016-06-15 10:41:56.784841

"""

# revision identifiers, used by Alembic.
revision = '9dce37322c71'
down_revision = ('960085fce39c', 'edc5377c32c7')
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_development():
    pass


def downgrade_development():
    pass


def upgrade_test():
    pass


def downgrade_test():
    pass


def upgrade_production():
    pass


def downgrade_production():
    pass

