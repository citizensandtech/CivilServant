/* RUN THIS FIRST */
DROP TABLE subreddit_page_utc_ids
DROP TABLE front_page_utc_ids
CREATE TABLE subreddit_page_utc_ids (id INTEGER);
CREATE TABLE front_page_utc_ids (id INTEGER);

/* THEN RUN THE SCRIPT*/

/* THEN RUN THESE QUERIES */

UPDATE front_pages fp RIGHT JOIN front_page_utc_ids ids ON (fp.id = ids.id) SET fp.is_utc=NULL;
UPDATE subreddit_pages sp RIGHT JOIN subreddit_page_utc_ids ids ON (sp.id = ids.id) SET sp.is_utc=NULL;
