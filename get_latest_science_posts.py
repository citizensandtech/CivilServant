import reddit.connection

r = reddit.connection.connect()

posts = []
sub = r.get_subreddit("science")
for post in sub.get_new(limit=100):
  posts.append(post)
  print(post.__dict__)
  print
