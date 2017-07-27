SET @cutoff = DATE_SUB(NOW(), INTERVAL 6 WEEK);
select count(*) from comments WHERE created_at < @cutoff;

INSERT INTO archived_comments SELECT * from COMMENTS WHERE comments.created_at < @cutoff;
select count(*) from archived_comments;

DELETE FROM comments WHERE comments.created_at < @cutoff;
select count(*) from comments;
