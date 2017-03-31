/* RUN THIS FIRST */
DROP TABLE subreddit_page_utc_ids
DROP TABLE front_page_utc_ids
CREATE TABLE subreddit_page_utc_ids (id INTEGER, is_utc INTEGER);
CREATE TABLE front_page_utc_ids (id INTEGER, is_utc INTEGER);

/* THEN RUN THE SCRIPT*/

/* THEN RUN THESE QUERIES */

UPDATE front_pages SET is_utc=NULL WHERE id in (select id from front_page_utc_ids);
UPDATE subreddit_pages SET is_utc=NULL WHERE id in (select id from subreddit_page_utc_ids);
