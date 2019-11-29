
# coding: utf-8

# ## Getting 10,000 urls from DMCA-COX
# -  ssh -N dmca@cox.media.mit.edu -L 3311:cox.media.mit.edu:3306

# In[1]:


from utils.common import DbEngine
import os
import inspect
from app.models import TwitterStatus, TwitterUnshortenedUrls
import json
from sqlalchemy.sql.expression import func as sqlfunc

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
ENV = 'production_over_ssh'
db_session = DbEngine(os.path.join(BASE_DIR, "config") + "/{env}.json".format(env=ENV)).new_session()
urls = db_session.query(TwitterUnshortenedUrls.unshortened_url).all()


urlsf = [u[0] for u in urls]



def domain_name(url):
    try:
        no_protocol = url.split('://')[1]
        before_first_slash = no_protocol.split("/")[0]
        # domain_tld = before_first_slash.split(".")[-2:]
        # '.'.join(domain_tld)
        # doesn't work for things like .org.cn
        return before_first_slash
    except AttributeError:
        print(url)


# In[28]:


domains = [domain_name(u) for u in urlsf if u]


# In[34]:


from collections import Counter
import pandas as pd


# In[32]:


domain_counts = Counter(domains)


# In[39]:


df = pd.DataFrame.from_dict(domain_counts, orient='index').reset_index().rename(columns={'index':'domain',"0":"frequency"})


# In[1]:


df.to_csv('domain_frequencies.csv', index=False)

