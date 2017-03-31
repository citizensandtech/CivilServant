select id, is_utc from subreddit_pages where is_utc is NULL OR is_utc = FALSE;
