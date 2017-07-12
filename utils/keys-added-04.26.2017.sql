create index comments_created_at ON comments(created_at);
create index frontpage_created_at ON front_pages(created_at);
create index mod_actions_created_at ON mod_actions(created_at);
create index posts_created_at ON posts(created_at);
create index comments_subreddit_id ON comments(subreddit_id);
create index mod_action_subreddit_id ON mod_actions(subreddit_id);
