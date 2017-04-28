import re, random, string, sys, math, os
BASEDIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../") 
sys.path.append(BASEDIR)
import simplejson as json
import app.connections.reddit_connect

# GENERATES FOUR PAGES PAGE OF ANONYMIZED FIXTURE DATA 
# FROM THE ACTUAL MODERATION LOG OF A SUBREDDIT
# USAGE: fetch_mod_log.py <<subreddit>> <<pages>>

subreddit = sys.argv[1]
pages = int(sys.argv[2])

def randstring(n):
  return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

conn = app.connections.reddit_connect
r = conn.connect(controller="ModLog")

actions =  [x.json_dict for x in r.get_mod_log(subreddit, limit=500)]
for i in range(0,pages-1):
  actions += [x.json_dict for x in r.get_mod_log(subreddit, limit=500, params={"after":actions[-1]['id']})]

print("Fetched {0} moderator actions from {1}".format(len(actions), subreddit))

replace_keys = {}
for key in ["mod", "mod_id36", "target_fullname", "target_title", "id", "target_author", "target_body", "target_permalink"]:
  unique_values = list(set([x[key] for x in actions]))
  unique_values = list(filter(None.__ne__, unique_values))
  replace_keys[key] = {}

  prefix = ""

  ## CHECK FOR R SCIENCE URL
  prefix_match = re.search("\/r\/.*?\/comments\/", unique_values[0])
  if(prefix_match):
      prefix = prefix_match.group(0)
  else: ## CHECK FOR FOR REDDIT FULLNAME
    prefix_match = re.search(".*?_", unique_values[0])
    if(prefix_match):
      prefix = prefix_match.group(0)

  for value in unique_values:
    replace_keys[key][value] = prefix + randstring(len(value) +len(prefix))


for action in actions:
  for key in replace_keys.keys():
    if(action[key] != None):
      action[key] = replace_keys[key][action[key]]

pagelimit = math.floor(len(actions) / pages)
head = 0
pagenum = 1
head_tail = []
while head + 1 <= len(actions):
 filename = os.path.join(BASEDIR, "tests", "fixture_data", "mod_actions_" + str(pagenum) + ".json")
 print("Writing {0}".format(filename))
 f = open(filename, "w")
 tail = math.floor(pagenum * pagelimit)
 f.write(json.dumps(actions[head:tail]))
 head_tail.append("{0}:{1}".format(head,tail))
 f.close()
 head = math.floor(pagenum*pagelimit) 
 pagenum += 1
