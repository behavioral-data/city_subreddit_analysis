#!/usr/bin/env python
# coding: utf-8

# In[12]:


import sys
import time


# In[2]:

SUB_NAMES = [
	'NYC',
	'Seattle',
	'LosAngeles',
	'Chicago',
	'Austin',
	'Portland',
	'SanFrancisco',
	'Boston',
	'Houston',
	'Atlanta',
	'Philadelphia',
	'Denver',
	'SeattleWa',
	'Dallas',
	'WashingtonDC',
	'SanDiego',
	'Pittsburgh',
	'Phoenix',
	'Minneapolis',
	'Orlando',
	'Nashville',
	'StLouis',
	'SaltLakeCity',
	'Columbus',
	'Raleigh',
	'NewOrleans',
	'Tampa',
	'KansasCity',
	'rva',
	'Charlotte',
	'Baltimore',
	'Detroit',
	'Vegas',
	'Indianapolis',
	'Cincinnati',
	'Miami',
	'Boulder',
	'Sacramento',
	'MadisonWi',
	'SanAntonio',
	'Cleveland',
	'Milwaukee',
	'Louisville',
	'Chattanooga',
	'LasVegas',
	'Buffalo',
	'Tucson',
	'Rochester',
	'FortWorth',
	'Albuquerque',
	'Charleston',
	'Tulsa',
	'Memphis',
	'Jacksonville',
	'Knoxville',
	'Albany',
	'bullcity',
	'DesMoines',
	'London',
	'Toronto',
	'Melbourne',
	'Vancouver',
	'Sydney',
	'Calgary',
	'Montreal',
	'Berlin',
]

sys.path.append('/homes/gws/gweld/reddit_moderator_recruiting')


# In[3]:


from SparkSetup import spark


# In[5]:


from pyspark.sql.types import *
from pyspark.sql import functions as F


# In[6]:


posts_schema = StructType([
    StructField('id',                   StringType()),
    StructField('subreddit',            StringType()),
    StructField('author',               StringType()),
    StructField('title',                StringType()),
    StructField('url',                  StringType()),
    StructField('selftext',             StringType()),
    StructField('created_utc',         IntegerType()),
    StructField('score',               IntegerType()),
])

comments_schema = StructType([
    StructField('id',                   StringType()),
    StructField('subreddit',            StringType()),
    StructField('author',               StringType()),
    StructField('body',                 StringType()),
    StructField('parent_id',            StringType()),
    StructField('created_utc',         IntegerType()),
    StructField('score',               IntegerType()),
])


# In[13]:


def filter_to_sub(path_to_load_from, schema, kind):
    start_time = time.monotonic()
    print('Loading posts from {}.'.format( path_to_load_from ))
    items = spark.read.format('json').schema(schema).option('mode', 'PERMISSIVE').load(path_to_load_from)

    items = items.filter(F.col('subreddit').isin(SUB_NAMES))

    print(f'Have {items.count():,d} {kind}s after filtering.')

    items.write.json(f'/projects/bdata/temp_city_subreddit_{kind}s_2020', mode='overwrite')
    print(f"Finished computing and writing output for {kind}s in {(time.monotonic()-start_time)/60:5.3f} minutes.")


# In[14]:


filter_to_sub('/projects/bdata/moderation/pushshift/RS_2020-*',    posts_schema, 'post'   )
filter_to_sub('/projects/bdata/moderation/pushshift/RC_2020-*', comments_schema, 'comment')

#filter_psbattles('/projects/bdata/moderation/pushshift/RC_2013-01', comments_schema, 'comment')


# In[ ]:




