import simplejson as json
import random, string, sys
from random import randint
import heapq

#anonymize_comment_bodies.py INFILE > OUTFILE

## NOTE: THIS DECOUPLES IDs and PARENT_IDs BY ANONYMIZING THEM

def randstring(n):
  return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

all_comments = []
user_keys = {}

with open(sys.argv[1]) as f:
  for line in f.readlines():
    row = json.loads(line)

    author = row['author']
    gen_author = None
    if author in user_keys.keys():
      gen_author = user_keys[author]
    else:
      gen_author = randstring(20)
      user_keys[author] = gen_author

    row['author'] = gen_author
    row['body'] = randstring(50)
    row['id']  = randstring(10)
    row['parent_id']  = randstring(10)
    row['created_utc'] = int(row['created_utc']) + randint(-50,50)
    all_comments.append(row)

## NOW HEAPSORT ITEMS BY TIMESTAMP AND OUTPUT
for item in sorted(all_comments, key=lambda a: a['created_utc']):
  print(json.dumps(item))
  
