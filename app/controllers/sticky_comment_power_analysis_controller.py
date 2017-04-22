import inspect, os, sys, pytz
import simplejson as json
import datetime
import numpy as np
from app.models import Base, Subreddit, ModAction, FrontPage
from sqlalchemy import and_

utc=pytz.UTC
COMMENTS_ACTIONS = set(["removecomment", "approvecomment", "spamcomment"])
REMOVE_COMMENTS_ACTIONS = set(["removecomment", "spamcomment"])
POSTS_ACTIONS = set(["removelink", "approvelink", "spamlink"])
REMOVE_POSTS_ACTIONS = set(["removelink", "spamlink"])

class StickyCommentPowerAnalysisController:
    def __init__(self, subreddit_id, start_date, end_date, data_dir, output_dir, db_session, log):

        self.start_date = start_date
        self.begin_date = self.start_date - datetime.timedelta(days=6*31) # about 6 months ago
        self.end_date = end_date

        self.data_dir = data_dir
        self.output_dir = output_dir

        self.posts = None
        self.comments = None
        self.mod_actions_comments = None
        self.mod_actions_posts = None
        self.post_to_comment_info = None
        self.frontpages = None

        self.db_session = db_session
        self.log = log

        self.subreddit = self.get_subreddit(subreddit_id)

    # calls create_datasets() and produces two different csv files, 
    # timestamped, from these lists of dicts
    #
    # expecting id of subbreddit, e.g. "2qh13"
    # dates passed as strings "MM.YYYY"
    # dates passed as strings "MM.YYYY"
    # "/mnt/samba/reddit-archive/03.2017"
    # start_date = datetime.datetime.strptime(start_date, "%m.%Y")
    # end_date = datetime.datetime.strptime(end_date, "%m.%Y")    
    def create_csvs(self, frontpage_limit=10):
        self.log.info("Creating csvs for posts and comments from {0} to {1}".format(self.begin_date, self.end_date))
        start_date_str = "{0}.{1}".format(self.start_date.month, self.start_date.year)
        end_date_str = "{0}.{1}".format(self.end_date.month, self.end_date.year)
        
        posts_fname = "sticky_comment_power_analysis_{0}_{1}_{2}_posts".format(self.subreddit.id, start_date_str, end_date_str)
        post_heading = ["id","created.utc","author","body.length","weekday","url","is.selftext","visible","num.comments","num.comments.removed","front_page","author.prev.posts","author.prev.participation"]
        with open(os.path.join(self.output_dir, posts_fname), "w") as f:
            f.write(",".join(post_heading) + "\n")

        comments_fname = "sticky_comment_power_analysis_{0}_{1}_{2}_comments".format(self.subreddit.id, start_date_str, end_date_str)
        comment_heading = ["id","created.utc","author","body.length","toplevel","post.id","visible","post.visible","post.author","author.prev.comments","author.prev.participation"]
        with open(os.path.join(self.output_dir, comments_fname), "w") as f:
            f.write(",".join(comment_heading) + "\n")

        self.create_datasets(frontpage_limit)
        
        with open(os.path.join(self.output_dir, posts_fname), "a") as f:
            for post_id in self.posts:
                row = [str(post_id)] + [str(self.posts[post_id][label]) for label in post_heading[1:]]
                f.write(",".join(row) + "\n")
        with open(os.path.join(self.output_dir, comments_fname), "a") as f:
            for comment_id in self.comments:
                row = [str(comment_id)] + [str(self.comments[comment_id][label]) for label in comment_heading[1:]]
                f.write(",".join(row) + "\n")

    def get_subreddit(self, subreddit_id):
        subreddit = self.db_session.query(Subreddit).filter(Subreddit.id == subreddit_id).first()
        return subreddit

    # Returns Two Lists of Dicts, Where Each Dict Contains One Row
    def create_datasets(self, frontpage_limit):
        # get posts, comments, modlog from (self.start_date - 6 months) to self.end_date

        self.log.info("Getting posts...")
        self.posts = self.get_posts()
        self.log.info("Getting comments...")
        self.comments = self.get_comments()

        self.log.info("Getting modlog...")
        (self.mod_actions_comments, self.mod_actions_posts) = self.get_modlog()
        self.post_to_comment_info = self.get_post_to_comment_info() # needs mod_actions
        self.log.info("Getting frontpages...")
        self.frontpages = self.get_frontpage_data()  


        # posts, comments = apply_post_flair(posts, comments)    # get post flair - don't do right now
        self.log.info("Applying modlog...")
        self.apply_mod_actions() # posts, comments   # get visible posts
        self.log.info("Applying frontpages...")
        self.apply_frontpage_data(frontpage_limit) # posts   # get front page minutes
        self.log.info("Applying participation and post to comment info...")
        #####self.apply_participation_and_post_to_comment_info() # posts, comments   # count prev posts


    # general note for post, comment creation: yet-to-be-processed field default to None.
    # in later methods (eg when applying modlog visibility information), fields will update to True/False
    def get_posts(self):
        posts_data_dir = os.path.join(self.data_dir, "posts") # note that it's looking here for posts!
        files = os.listdir(self.data_dir)
        my_post_files = []
        for f in files:
            f_str = f.split(".")
            # expecting strings like "reddit.posts.08.2016.000000000042.json"
            if f_str[0] == "reddit" and f_str[1] == "posts" and f_str[-1] == "json":
                month = f_str[2]
                year = f_str[3]
                date = datetime.datetime.strptime("{0}.{1}".format(month, year), "%m.%Y")
                if date - self.begin_date >= datetime.timedelta(0) and self.end_date - date >= datetime.timedelta(0):
                    my_post_files.append(f)

        self.log.info("Looking in {0} posts files".format(len(my_post_files)))
        all_posts = {} # post id: post_data
        for file in my_post_files:
            with open(os.path.join(posts_data_dir, file), "r") as lines:
                for line in lines:
                    post = json.loads(line)
                    if "subreddit" in post and post["subreddit"] == self.subreddit.name:
                        post_id = post["name"].replace("t3_", "")
                        date = utc.localize(datetime.datetime.utcfromtimestamp(float(post['created_utc'])))
                        post_data = {
                            "created.utc": date,
                            "weekday": date.weekday(),  # day of the week as an integer, where Monday is 0 and Sunday is 6
                            "author": post["author"],
                            "body.length": len(post["body"].split(" ")),
                            "is.selftext": post["is_self"],
                            "url": post["url"] if post["is_self"] else None, # url of link (if link)
                            "author.deleted.later": post["author"] == "[deleted]",

                            "visible": None, #was the post allowed to persist by moderators?

                            "author.prev.posts": None, # number of previous posts of any kind by this author in this subreddit...   
                            "author.prev.participation": None, # number of previous posts or comments of any kind by this author in this subreddit...  
                            "num.comments": None, # number of comments received by this post
                            "num.comments.removed": None, # number of comments that were removed

                            "newcomer.comments": None, # number of newcomer comments received by this post
                            "newcomer.comments.removed": None, # number of newcomer comments removed

                            "front_page": None, # number of minutes that the post appeared on the front page

                            #"post.flair": None, # need to query API
                        }

                        all_posts[post_id] = post_data
        self.log.info("{0} posts loaded".format(len(all_posts)))
        return all_posts

    def get_comments(self):
        files = os.listdir(self.data_dir)
        my_comment_files = []
        for f in files:
            f_str = f.split(".")
            # expecting strings like "reddit.comments.08.2016.000000000042.json"
            if f_str[0] == "reddit" and f_str[1] == "comments" and f_str[-1] == "json":
                month = f_str[2]
                year = f_str[3]
                date = datetime.datetime.strptime("{0}.{1}".format(month, year), "%m.%Y")
                if date - self.begin_date >= datetime.timedelta(0) and self.end_date - date >= datetime.timedelta(0):
                    my_comment_files.append(f)


        self.log.info("Looking in {0} comments files".format(len(my_comment_files)))
        all_comments = {} # comment id: post_data
        for file in my_comment_files:
            with open(os.path.join(self.data_dir, file), "r") as lines:
                for line in lines:
                    comment = json.loads(line)
                    if "subreddit" in comment and comment["subreddit"] == self.subreddit.name:
                        comment_id = comment["id"]
                        date = utc.localize(datetime.datetime.utcfromtimestamp(float(comment['created_utc'])))
                        comment_data = {
                            "created.utc": date,
                            "author": comment["author"],
                            "body.length": len(comment["body"].split(" ")),
                            "toplevel": comment["link_id"] == comment["parent_id"], # Is this comment toplevel or not?
                            "author.deleted.later": comment["author"] == "[deleted]",
                            
                            "post.id": comment["link_id"], # t3
                                        
                            "author.prev.comments": None, # number of previous comments of any kind by this author in this subreddit, in the observed datasets -- a value of 0 means this is their very first comment
                            "author.prev.participation": None, # number of previous comments or posts of any kind by this author in this subreddit....

                            "visible": None, #was the post allowed to persist by moderators?
                            "post.visible": None, # Was the post removed?
                            #"post.flair": None, # need to query API
                            "post.author": None # Who was the post author
                        }

                        all_comments[comment_id] = comment_data

        self.log.info("{0} comments loaded".format(len(all_comments)))
        return all_comments

    def get_modlog(self):
        rows = self.db_session.query(ModAction).filter(and_(ModAction.subreddit_id == self.subreddit.id, ModAction.created_utc >= self.begin_date)).order_by(ModAction.created_utc).all()
        mod_actions = []
        for row in rows:
            mod_action = json.loads(row['action_data']) # a dict
            mod_action['created'] = utc.localize(datetime.datetime.utcfromtimestamp(mod_action['created_utc']))
            mod_actions.append(mod_action)
        self.log.info("Loaded {0} mod actions total".format(len(mod_actions)))


        mod_actions_comments = {} # comment_id: [action, action, ...]
        mod_actions_posts = {} # post_id: [action, action, ...]

        # Load mod_actions_comments, mod_actions_posts
        for action in mod_actions:
            # regarding a comment
            if action['action'] in COMMENTS_ACTIONS:
                comment_id = action['target_fullname'].replace("t1_", "")
                if comment_id not in mod_actions_comments:
                    mod_actions_comments[comment_id] = []   
                mod_actions_comments[comment_id].append(action)

            # regarding a post
            if action['action'] in POSTS_ACTIONS:
                post_id = action['target_fullname'].replace("t3_","")
                if post_id not in mod_actions_posts:
                    mod_actions_posts[post_id] = []
                mod_actions_posts[post_id].append(action)               

        self.log.info("Loaded {0} mod actions on comments".format(len(mod_actions_comments)))
        self.log.info("Loaded {0} mod actions on posts".format(len(mod_actions_posts)))        
        
        return mod_actions_comments, mod_actions_posts

    def get_post_to_comment_info(self):
        # Updates comment["visible"], get info for posts' 2nd update
        post_to_comment_info = {} # post_id: {"comments": set([id, id, ...]), "removed_comments": set([id, id, ...])}
        for comment_id in self.mod_actions_comments:
            actions = self.mod_actions_comments[comment_id]
            if comment_id in self.comments:
                comment = self.comments[comment_id]
                for action in actions:
                    post_id = comment["post.id"].replace("t3_","")
                    if post_id not in post_to_comment_info:
                        post_to_comment_info[post_id] = {"comments": set([]), "removed_comments": set([])}
                    post_to_comment_info[post_id]["comments"].add(comment_id)

                    if action['action'] == "removecomment" or action['action'] == "spamcomment":
                        post_to_comment_info[post_id]["removed_comments"].add(comment_id)
                    elif action['action'] == "approvecomment":
                        post_to_comment_info[post_id]["removed_comments"].discard(comment_id)        

        self.log.info("Found {0} posts that have comments".format(len(post_to_comment_info)))
        return post_to_comment_info



    def get_frontpage_data(self):
        rows = self.db_session.query(FrontPage).filter(FrontPage.created_at >= self.begin_date).order_by(FrontPage.created_at).all()
        frontpage_data = []
        for row in rows:
            frontpage['data'] = json.loads(row['page_data']) # a list
            frontpage['created'] = utc.localize(datetime.datetime.utcfromtimestamp(row['created_at']))
            recent_frontpages.append(frontpage)
        self.log.info("Loaded {0} frontpage records".format(len(frontpage_data)))            
        return frontpage_data


    def apply_mod_actions_visible(self, is_comments):
        num_approve_actions = 0
        num_remove_actions = 0
        num_items_with_mod_actions = set([])
        num_items_removed_at_least_once = set([])

        item_str = "comments" if is_comments else "posts"
        items = self.comments if is_comments else self.posts
        mod_actions_items = self.mod_actions_comments if is_comments else self.mod_actions_posts
        remove_items_actions = REMOVE_COMMENTS_ACTIONS if is_comments else REMOVE_POSTS_ACTIONS

        # Updates comment["visible"]
        for item_id in mod_actions_items:
            actions = mod_actions_items[item_id]
            if item_id in self.items:
                item = self.items[item_id]
                num_items_with_mod_actions.add(item_id)
                for action in actions:
                    ## many authors are later deleted, so try to 
                    ## add in the author information here, since
                    ## the moderation log retains the author information
                    item['author']  = action['target_author'] # updates author name
                    if is_comment:
                        post_id = item["post.id"].replace("t3_","")


                    if action['action'] in remove_items_actions:
                        item['visible'] = False
                        num_remove_actions += 1
                        num_items_removed_at_least_once.add(item_id)
                    elif action['action'] not in remove_items_actions:
                        item['visible'] = True
                        num_approve_actions += 1

        self.log.info("{0} mod log approve {1} actions".format(num_approve_actions, item_str))
        self.log.info("{0} mod log remove {1} actions".format(num_remove_actions, item_str))
        self.log.info("{0} {1} with mod actions".format(len(num_items_with_mod_actions), item_str))
        self.log.info("{0} {1} were removed at least once".format(len(num_items_removed_at_least_once), item_str))

    def apply_mod_actions(self):

        self.apply_mod_actions_visible(is_comments=True)
        self.apply_mod_actions_visible(is_comments=False)  
        
        # Updates comment["post.author"], comment["post.visible"]            
        for comment_id in self.comments:
            comment = self.comments[comment_id]
            post_id = comment["post.id"].replace("t3_","")
            comment["post.visible"] = self.posts[post_id]["visible"] if post_id in self.posts else None
            comment["post.author"] = self.posts[post_id]["num.comments.removed"] if post_id in self.posts else None


    def apply_frontpage_data(self, limit):

        post_to_timestamps = {} # {post_id: (min_timestamp, max_timestamp)}

        for page in self.frontpages:
            this_time = page["created"]
            for i, item in page['data'][:limit]:
                post_id = item["id"]
                if post_id in self.posts:
                    if post_id not in posts_to_timestamps:
                        posts_to_timestamps[post_id] = (this_time, this_time)
                    (min_time, max_time) = posts_to_timestamps[post_id] 
                    posts_to_timestamps[post_id] = (min(min_time, this_time), max(min_time, this_time))

        for post_id in self.posts:
            post = self.posts[post_id]
            post["front_page"] = 0
            if post_id in post_to_timestamps:
                (min_time, max_time) = posts_to_timestamps[post_id]
                post["front_page"] = int((max_time - min_time).total_seconds()/60)

        self.log.info("{0} posts appeared on frontpage".format(len(post_to_timestamps)))


    # for each comment, add author previous comment count 
    # (first observed comment by that author is 0, second is 1, etc)
    def apply_participation_and_post_to_comment_info(self):
        # fields that this function updates:
        # comments: "author.prev.comments", "author.prev.participation"
        # posts: "num.comments", "num.comments.removed", "author.prev.posts", "author.prev.participation", "newcomer.comments", "newcomer.comments.removed"


        # order all posts and comments chronologically in a somewhat inefficient but relatively clear way
        sorted_comments = [{
            "id": comment_id, 
            "author": self.comments[comment_id]["author"],
            "is_post": False, 
            "created_at": self.comments[comment_id]["created.utc"]} for comment_id in sorted(self.comments, key=lambda c: self.comments[c]["created.utc"])]
        sorted_posts = [{ 
            "id": post_id, 
            "author": self.posts[post_id]["author"],
            "is_post": True, 
            "created_at": self.posts[post_id]["created.utc"]} for post_id in sorted(self.posts, key=lambda p: self.posts[p]["created.utc"])]
        all_items = sorted_comments + sorted_posts
        sorted_items = sorted(all_items, key=lambda x: all_items[x]["created_at"])

        # update participation, then update fields: 
        participation = {} # {author: {"num_posts": counter, "num_comments": counter}}
        for item in sorted_items:   
            author = item["author"]
            if author not in participation:
                participation[author] = {"num_posts": 0, "num_comments": 0}
            if item["is_post"]:
                post_id = item["id"]
                post = self.posts[post_id]

                participation[author]["num_posts"] += 1

                # update "author.prev.posts", "author.prev.participation"
                post["author.prev.posts"] = participation[author]["num_posts"]
                post["author.prev.participation"] = participation[author]["num_posts"] + participation[author]["num_comments"]

            else:
                comment_id = item["id"]
                seen_comment_ids.add(comment_id)
                comment = self.comments[comment_id]
                participation[author]["num_comments"] += 1

                # update "author.prev.comments", "author.prev.participation"
                comment["author.prev.comments"] = participation[author]["num_comments"]
                comment["author.prev.participation"] = participation[author]["num_posts"] + participation[author]["num_comments"]


        def is_newcomer(author, participation):
            return (author not in participation) or (participation[author]["num_posts"] == 0 and participation[author]["num_comments"] == 0)


        # using final participation dict, self.post_to_comment_info
        for post_id in self.posts:
            post = self.posts[post_id]

            # update "num.comments", "num.comments.removed" fields
            post["num.comments"] = 0
            post["num.comments.removed"] = 0
            if post_id in self.post_to_comment_info:
                post["num.comments"] = self.post_to_comment_info["comments"]
                post["num.comments.removed"] = self.post_to_comment_info["removed_comments"]

            # "newcomer.comments", "newcomer.comments.removed"
            post["newcomer.comments"] = 0
            post["newcomer.comments.removed"] = 0
            if post_id in self.post_to_comment_info:
                info = self.post_to_comment_info[post_id]

                # total number of comments that come from authors that are comment newcomers
                # self.post_to_comment_info[post_id] contains the final list of ids of "commments" and "removed_comments", constructed from self.comments and self.mod_actions_comments 
                posts[post_id]["newcomer.comments"] = [cid for cid in info["comments"] if cid in seen_comment_ids and is_newcomer(self.comments[cid]["author"], participation)]
                posts[post_id]["newcomer.comments.removed"] = [cid for cid in info["removed_comments"] if cid in seen_comment_ids and is_newcomer(self.comments[cid]["author"], participation)]



    # comment newcommers: 
    # for each comment, if comment["author.prev.comments"] == 0, then newcomer.

    #for each post, for each comment, if comment.post.id == post_id and prev.comments == 0, then +1