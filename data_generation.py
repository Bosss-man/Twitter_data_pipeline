# Header Files

import tweepy
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace
from pyspark.rdd import RDD
from pyspark.sql.functions import expr,col,column
import pyspark.sql.functions as sql_func
import pyspark.sql.functions as pf


spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.maxLines", 100000)
spark.conf.set("spark.sql.repl.eagerEval.truncate", 0)


#Extracting tweets from twitter API

#Connection to twitter api and reading the tweets

# VARIABLES
consumer_key = 'ab'
consumer_secret = 'ab'


# CONNECTION
# Authenticate to Twitter
auth = tweepy.OAuth1UserHandler(
    consumer_key = 'abc',
    consumer_secret = 'abc',
    access_token = '316223746-ab',
    access_token_secret = 'ab'
)

# Create API object
api = tweepy.API(auth)

#verifying the connection
#api.verify_credentials()

#posting tweet 
# api.update_status('Working on a project connecting - twitter data, pyspark, and azure #dataengineer #data #projects #coding #coders')

#Required variables to store the tweets
hashtags = []
refined_tweets = []

#Reading the tweets 
hashtag = ['dataengineering']
tweets = api.search_tweets(q = hashtag, result_type = 'recent',count = 250,tweet_mode='extended')
tweets_list = []
for tweet in tweets:    
    if 'retweeted_status' in tweet._json:
        #print(tweet._json['retweeted_status']['full_text'])
        continue
    else:
        tweets_list.append(tweet.full_text)
