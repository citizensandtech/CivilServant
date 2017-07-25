"""Added indices on created_at

Revision ID: 05a831a5db7b
Revises: a571e57d884a
Create Date: 2017-07-24 23:44:23.301874

"""

# revision identifiers, used by Alembic.
revision = '05a831a5db7b'
down_revision = 'a571e57d884a'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_development():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f('ix_comments_created_at'), 'comments', ['created_at'], unique=False)
    op.create_index(op.f('ix_event_hooks_created_at'), 'event_hooks', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_actions_created_at'), 'experiment_actions', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_thing_snapshots_created_at'), 'experiment_thing_snapshots', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_things_created_at'), 'experiment_things', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiments_created_at'), 'experiments', ['created_at'], unique=False)
    op.create_index(op.f('ix_front_pages_created_at'), 'front_pages', ['created_at'], unique=False)
    op.create_index(op.f('ix_mod_actions_created_at'), 'mod_actions', ['created_at'], unique=False)
    op.create_index(op.f('ix_posts_created_at'), 'posts', ['created_at'], unique=False)
    op.create_index(op.f('ix_praw_keys_created_at'), 'praw_keys', ['created_at'], unique=False)
    op.create_index(op.f('ix_subreddit_pages_created_at'), 'subreddit_pages', ['created_at'], unique=False)
    op.create_index(op.f('ix_subreddits_created_at'), 'subreddits', ['created_at'], unique=False)
    ### end Alembic commands ###


def downgrade_development():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_subreddits_created_at'), table_name='subreddits')
    op.drop_index(op.f('ix_subreddit_pages_created_at'), table_name='subreddit_pages')
    op.drop_index(op.f('ix_praw_keys_created_at'), table_name='praw_keys')
    op.drop_index(op.f('ix_posts_created_at'), table_name='posts')
    op.drop_index(op.f('ix_mod_actions_created_at'), table_name='mod_actions')
    op.drop_index(op.f('ix_front_pages_created_at'), table_name='front_pages')
    op.drop_index(op.f('ix_experiments_created_at'), table_name='experiments')
    op.drop_index(op.f('ix_experiment_things_created_at'), table_name='experiment_things')
    op.drop_index(op.f('ix_experiment_thing_snapshots_created_at'), table_name='experiment_thing_snapshots')
    op.drop_index(op.f('ix_experiment_actions_created_at'), table_name='experiment_actions')
    op.drop_index(op.f('ix_event_hooks_created_at'), table_name='event_hooks')
    op.drop_index(op.f('ix_comments_created_at'), table_name='comments')
    ### end Alembic commands ###


def upgrade_test():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f('ix_comments_created_at'), 'comments', ['created_at'], unique=False)
    op.create_index(op.f('ix_event_hooks_created_at'), 'event_hooks', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_actions_created_at'), 'experiment_actions', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_thing_snapshots_created_at'), 'experiment_thing_snapshots', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_things_created_at'), 'experiment_things', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiments_created_at'), 'experiments', ['created_at'], unique=False)
    op.create_index(op.f('ix_front_pages_created_at'), 'front_pages', ['created_at'], unique=False)
    op.create_index(op.f('ix_mod_actions_created_at'), 'mod_actions', ['created_at'], unique=False)
    op.create_index(op.f('ix_posts_created_at'), 'posts', ['created_at'], unique=False)
    op.create_index(op.f('ix_praw_keys_created_at'), 'praw_keys', ['created_at'], unique=False)
    op.create_index(op.f('ix_subreddit_pages_created_at'), 'subreddit_pages', ['created_at'], unique=False)
    op.create_index(op.f('ix_subreddits_created_at'), 'subreddits', ['created_at'], unique=False)
    ### end Alembic commands ###


def downgrade_test():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_subreddits_created_at'), table_name='subreddits')
    op.drop_index(op.f('ix_subreddit_pages_created_at'), table_name='subreddit_pages')
    op.drop_index(op.f('ix_praw_keys_created_at'), table_name='praw_keys')
    op.drop_index(op.f('ix_posts_created_at'), table_name='posts')
    op.drop_index(op.f('ix_mod_actions_created_at'), table_name='mod_actions')
    op.drop_index(op.f('ix_front_pages_created_at'), table_name='front_pages')
    op.drop_index(op.f('ix_experiments_created_at'), table_name='experiments')
    op.drop_index(op.f('ix_experiment_things_created_at'), table_name='experiment_things')
    op.drop_index(op.f('ix_experiment_thing_snapshots_created_at'), table_name='experiment_thing_snapshots')
    op.drop_index(op.f('ix_experiment_actions_created_at'), table_name='experiment_actions')
    op.drop_index(op.f('ix_event_hooks_created_at'), table_name='event_hooks')
    op.drop_index(op.f('ix_comments_created_at'), table_name='comments')
    ### end Alembic commands ###


def upgrade_production():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f('ix_comments_created_at'), 'comments', ['created_at'], unique=False)
    op.create_index(op.f('ix_event_hooks_created_at'), 'event_hooks', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_actions_created_at'), 'experiment_actions', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_thing_snapshots_created_at'), 'experiment_thing_snapshots', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiment_things_created_at'), 'experiment_things', ['created_at'], unique=False)
    op.create_index(op.f('ix_experiments_created_at'), 'experiments', ['created_at'], unique=False)
    op.create_index(op.f('ix_front_pages_created_at'), 'front_pages', ['created_at'], unique=False)
    op.create_index(op.f('ix_mod_actions_created_at'), 'mod_actions', ['created_at'], unique=False)
    op.create_index(op.f('ix_posts_created_at'), 'posts', ['created_at'], unique=False)
    op.create_index(op.f('ix_praw_keys_created_at'), 'praw_keys', ['created_at'], unique=False)
    op.create_index(op.f('ix_subreddit_pages_created_at'), 'subreddit_pages', ['created_at'], unique=False)
    op.create_index(op.f('ix_subreddits_created_at'), 'subreddits', ['created_at'], unique=False)
    ### end Alembic commands ###


def downgrade_production():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_subreddits_created_at'), table_name='subreddits')
    op.drop_index(op.f('ix_subreddit_pages_created_at'), table_name='subreddit_pages')
    op.drop_index(op.f('ix_praw_keys_created_at'), table_name='praw_keys')
    op.drop_index(op.f('ix_posts_created_at'), table_name='posts')
    op.drop_index(op.f('ix_mod_actions_created_at'), table_name='mod_actions')
    op.drop_index(op.f('ix_front_pages_created_at'), table_name='front_pages')
    op.drop_index(op.f('ix_experiments_created_at'), table_name='experiments')
    op.drop_index(op.f('ix_experiment_things_created_at'), table_name='experiment_things')
    op.drop_index(op.f('ix_experiment_thing_snapshots_created_at'), table_name='experiment_thing_snapshots')
    op.drop_index(op.f('ix_experiment_actions_created_at'), table_name='experiment_actions')
    op.drop_index(op.f('ix_event_hooks_created_at'), table_name='event_hooks')
    op.drop_index(op.f('ix_comments_created_at'), table_name='comments')
    ### end Alembic commands ###
