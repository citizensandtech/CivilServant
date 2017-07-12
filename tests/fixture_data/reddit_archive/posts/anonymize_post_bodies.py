import simplejson as json
import random, string, sys
from random import randint
import heapq

#anonymize_post_bodies.py INFILE > OUTFILE

def randstring(n):
  return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

all_objects = []
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
    row['title'] = randstring(50)
    row['permalink'] = randstring(50)
    row['domain'] = randstring(50)
    row['selftext'] = randstring(50)
    row['thumbnail'] = randstring(50)
    row['url'] = randstring(50)
    row['created_utc'] = int(row['created_utc']) + randint(-50,50)
    all_objects.append(row)

## NOW HEAPSORT ITEMS BY TIMESTAMP AND OUTPUT
for item in sorted(all_objects, key=lambda a: a['created_utc']):
  print(json.dumps(item))
  
