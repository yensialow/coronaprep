# Databricks notebook source
import pickle
import tweepy as tw
import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 2000)
pd.set_option('display.width', 2000)

def twitter_api():
    consumer_key = "ejD3rnJzJPrk4pomI6Jj7xP7g"
    consumer_secret = "F8ylc7Eogk1IX4NMV47xI21fJ7gh8U8Kg5AGfIvmCizwg6YkjC"
    access_token = "69460295-I8ujKvCbwrluNsmIha7uPypSFm5dEaHm9BLIlOJnn"
    access_secret = "KgER7Ci98IR9cVej86N9BRqd6hfkPgx3ywHDqn7ndvfeA"
    return consumer_key, consumer_secret, access_token, access_secret

# COMMAND ----------

#import API keys
# %run /Users/yensia.low@rallyhealth.com/config

# COMMAND ----------

#https://bhaskarvk.github.io/2015/01/how-to-use-twitters-search-rest-api-most-effectively./
consumer_key, consumer_secret, access_token, access_secret = twitter_api()
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# COMMAND ----------

# Define the search term and the date_since date as variables
#https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets
searchQuery = "(prep OR prepare OR preparation) (covid OR coronavirus OR virus) -filter:retweets"
maxTweets = 2*17000 # Some arbitrary large number
tweetsPerQry = 100  # this is the max the API permits
# since_id = -float('inf') #Returns results after this ID (i.e. more recent)
max_id = float('inf') #Returns results before this ID (i.e. older)

# filenames
local_path = 'file:/databricks/driver/'
dbfs_path = 'dbfs:/tmp/yen/'
f_tweets = 'tweets.pkl'
# f_df_tweets = 'df_tweets.pkl'

# COMMAND ----------

# Download f_tweets file from DBFS to local instance
#dbutils.fs.ls('dbfs:/tmp/yen')
#dbutils.fs.rm(local_path+f_tweets)
#dbutils.fs.cp(local_path+f_tweets, dbfs_path+f_tweets, recurse=True)
dbutils.fs.cp(dbfs_path+f_tweets, local_path+f_tweets, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download tweets

# COMMAND ----------

new_tweets = api.search(tweet_mode="extended", q=searchQuery, count=tweetsPerQry, lang='en', result_type='recent', max_id=max_id)

# COMMAND ----------

max_id = min([max_id] + [t.id for t in new_tweets])
max_id

# COMMAND ----------

max_id = min([max_id] + [t.id for t in new_tweets])
max_id

# COMMAND ----------

since_id = min([since_id] + [t.id for t in new_tweets])
since_id

# COMMAND ----------

len(new_tweets)

# COMMAND ----------

print("Start downloading up to {0} tweets...".format(maxTweets))
tweetCount = 0
while tweetCount < maxTweets:
    try:
        new_tweets = api.search(tweet_mode="extended", q=searchQuery, count=tweetsPerQry, lang='en', result_type='recent', max_id=max_id)
            
        if not new_tweets:
            print("No more tweets found")
            break
            
        else:
          #Save/dump everything ASAP
          with open(f_tweets, 'a+b') as f:
              pickle.dump(new_tweets, f)
          # for tweet in new_tweets:
          #     f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')

          max_id = min([max_id] + [t.id for t in new_tweets])
          tweetCount += len(new_tweets)
          print("Downloaded {0} tweets".format(tweetCount))

    except tw.TweepError as e:
        print("some error : " + str(e)) # Just exit if any error
        break

print("Downloaded {0} tweets total - saved to {1}".format(tweetCount, f_tweets))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Concat tweets from tweetstream

# COMMAND ----------

tweets = []
with open(f_tweets, 'rb') as fr:
    try:
        while True:
            tweets += pickle.load(fr)
    except EOFError:
        pass

# COMMAND ----------

print(len(tweets))

# COMMAND ----------

# Get the selected fields
tweets_sel = []
for tweet in tweets:
    tweets_sel.append([
                        tweet.id
                        ,tweet.full_text
                        ,tweet.created_at
                        ,tweet.user.id
                        ,tweet.user.name
                        ,tweet.user.screen_name
                        ,tweet.user.location
                        ,tweet.user.statuses_count
                        ,tweet.user.verified
                        ,tweet.favorite_count
                        ,tweet.favorited
                        ,tweet.retweet_count
                        ,tweet.entities
                        #,tweet.entities['hashtags']
                        #,tweet.entities['user_mentions']['screen_name']
                    ])

colnames = [
            'id'
            ,'text'
            ,'created_ts'
            ,'user_id'
            ,'user_name'
            ,'user_screen_name'
            ,'user_location'
            ,'user_statuses_count'
            ,'user_verified'
            ,'favorite_count'
            ,'favorited'
            ,'retweet_count'
            ,'entities'
            ]

df_tweets = pd.DataFrame(data=tweets_sel, columns=colnames)
df_tweets = df_tweets.set_index('id')
#df_tweets.head()
df_tweets.loc[df_tweets.index.duplicated(False)].sort_index()

# COMMAND ----------

#df_tweets = df_tweets.set_index('id')
df_tweets.loc[1235390247699206144]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save selected fields as pandas DF

# COMMAND ----------

# with open(f_df_tweets, 'wb') as f:
#     pickle.dump(df_tweets, f)

# COMMAND ----------

# dir(tweet)
# ['__class__',
#  '__delattr__',
#  '__dict__',
#  '__dir__',
#  '__doc__',
#  '__eq__',
#  '__format__',
#  '__ge__',
#  '__getattribute__',
#  '__getstate__',
#  '__gt__',
#  '__hash__',
#  '__init__',
#  '__init_subclass__',
#  '__le__',
#  '__lt__',
#  '__module__',
#  '__ne__',
#  '__new__',
#  '__reduce__',
#  '__reduce_ex__',
#  '__repr__',
#  '__setattr__',
#  '__sizeof__',
#  '__str__',
#  '__subclasshook__',
#  '__weakref__',
#  '_api',
#  '_json',
#  'author',
#  'contributors',
#  'coordinates',
#  'created_at',
#  'destroy',
#  'entities',
#  'favorite',
#  'favorite_count',
#  'favorited',
#  'geo',
#  'id',
#  'id_str',
#  'in_reply_to_screen_name',
#  'in_reply_to_status_id',
#  'in_reply_to_status_id_str',
#  'in_reply_to_user_id',
#  'in_reply_to_user_id_str',
#  'is_quote_status',
#  'lang',
#  'metadata',
#  'parse',
#  'parse_list',
#  'place',
#  'retweet',
#  'retweet_count',
#  'retweeted',
#  'retweeted_status',
#  'retweets',
#  'source',
#  'source_url',
#  'text',
#  'truncated',
#  'user']