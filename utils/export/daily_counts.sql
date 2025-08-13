select date(created_at), min(created_at), max(created_at), count(*) as comment_count from
comments group by date(created_at);

