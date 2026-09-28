"""Create subbreddit_id composite indexes

Revision ID: 5173f5fe666d
Revises: 8d2a661ad4b3
Create Date: 2026-09-11 13:23:33.844112

"""

# revision identifiers, used by Alembic.
revision = '5173f5fe666d'
down_revision = '8d2a661ad4b3'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()
    op.create_index(
        "ix_post_subreddit_id_created",
        "posts",
        ["subreddit_id", "created"])
    op.create_index(
        "ix_comments_subreddit_id_created_utc",
        "comments",
        ["subreddit_id", "created_utc"])
    op.create_index(
        "ix_mod_actions_subreddit_id_action_created",
        "mod_actions",
        ["subreddit_id", "action", "created_utc"])
    op.create_index(
        "ix_subreddit_pages_subreddit_id_created_at",
        "subreddit_pages",
        ["subreddit_id", "created_at"])
    op.create_index(
        "ix_experiment_things_experiment_id_created_at",
        "experiment_things",
        ["experiment_id", "created_at"])
    op.create_index(
        "ix_experiment_things_experiment_id_object_type",
        "experiment_things",
        ["experiment_id", "object_type"])
    op.create_index(
        "ix_experiment_things_experiment_id_query_index",
        "experiment_things",
        ["experiment_id", "query_index"])
    op.create_index(
        "ix_experiment_thing_snapshots_experiment_id_created_at",
        "experiment_thing_snapshots",
        ["experiment_id", "created_at"])
    op.create_index(
        "ix_experiment_thing_snapshots_experiment_id_object_type",
        "experiment_thing_snapshots",
        ["experiment_id", "object_type"])


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()
    op.drop_index("ix_post_subreddit_id_created")
    op.drop_index("ix_comments_subreddit_id_created_utc")
    op.drop_index("ix_mod_actions_subreddit_id_action_created")
    op.drop_index("ix_subreddit_pages_subreddit_id_created_at")
    op.drop_index("ix_experiment_things_experiment_id_created_at")
    op.drop_index("ix_experiment_things_experiment_id_object_type")
    op.drop_index("ix_experiment_things_experiment_id_query_index")
    op.drop_index("ix_experiment_thing_snapshots_experiment_id_created_at")
    op.drop_index("ix_experiment_thing_snapshots_experiment_id_object_type")


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

