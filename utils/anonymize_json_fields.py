import random, sys, string
import simplejson as json

filename = sys.argv[1]
key = sys.argv[2]

def randstring(n):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

jsonlist = []
with open(filename, "r") as f:
    jsonlist = json.loads(f.read())

## create list of unique values
uniquevalues = set()
for item in jsonlist:
    uniquevalues.add(item[key])

## create a dict of substitutes for the unique values
substitutes = {}
for value in uniquevalues:
    substitutes[value] = randstring(len(value))

## substitute the values
for item in jsonlist:
    item[key] = substitutes[item[key]]

## write the output to file
f = open(filename, "w")
f.write(json.dumps(jsonlist))
f.close()
