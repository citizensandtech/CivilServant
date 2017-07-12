import simplejson as json
import random
import sys
import heapq


#reduce_comments_json.py SUBREDDIT NONSUB_COUNT INFILE > OUTFILE

subreddit = sys.argv[1]
nonsub_count = int(sys.argv[2])

sub_items = []
nonsub_items = []

with open(sys.argv[3]) as f:
  for line in f.readlines():
    row = json.loads(line)
    if('subreddit_id' in row.keys() and row['subreddit_id'].replace("t5_","") == subreddit):
      sub_items.append(row)
    else:
      nonsub_items.append(row) 

return_items = sub_items
return_items += random.sample(nonsub_items, nonsub_count)

## NOW HEAPSORT ITEMS BY TIMESTAMP AND OUTPUT
for item in sorted(return_items, key=lambda a: a['created_utc']):
  print(json.dumps(item))

