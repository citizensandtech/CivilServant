### ADDED AN MULTICOLUMN INDEX FOR THE COMMENTS QUERY
CREATE INDEX ix_comments_subreddit_id_created_at ON comments(subreddit_id,created_at);
