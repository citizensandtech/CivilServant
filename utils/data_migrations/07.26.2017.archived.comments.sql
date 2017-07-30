INSERT INTO archived_comments select * from comments WHERE created_at < "2016-09-01";
DELETE FROM comments WHERE created_at < "2016-09-01";

INSERT INTO archived_comments select * from comments WHERE created_at < "2017-01-01"
/*DELETE FROM comments WHERE created_at < "2017-01-01";*/



SET @cutoff = DATE_SUB(NOW(), INTERVAL 6 WEEK);
select count(*) from comments WHERE created_at < @cutoff;

INSERT INTO archived_comments SELECT * from comments WHERE comments.created_at < @cutoff;
select count(*) from archived_comments;

DELETE FROM comments WHERE comments.created_at < @cutoff;
select count(*) from comments;
