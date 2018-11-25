from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import time
import findspark
findspark.init()

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from kafka import KafkaProducer

def streamer_job():

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    #consumer key, consumer secret, access token, access secret.
    ckey="zQF1YoRRmPZhuKw6LWz4VyoY1"
    csecret="Lsf7dwpzfmLMn6s9bEpArmDefar2UFrjRAhpxstH7ijpUfJeK2"
    atoken="1017435190556323840-qK4GWsSupuIOYSw6gSiTd64oINtc6a"
    asecret="MFe6P1Iq1r15HfN3PKtUCntfHb0pQULlYrsQXddfTx8y2"

    my_spark = SparkSession.builder.appName("twitterData").master("local").getOrCreate()
    sc = my_spark.sparkContext

    class listener(StreamListener):

        def on_data(self, data):
            try:
                all_data = json.loads(data)

                tweet = all_data["text"].lower()
                print(tweet)
                future = producer.send('tweets', bytes(tweet, encoding= 'utf-8'))
                producer.flush()

                return True
            except:
                return True

        def on_error(self, status):
            print(status)

    movies = my_spark.read.format("com.mongodb.spark.sql.DefaultSource") \
              .option("uri","mongodb://localhost:27017/movieDatabase.movies").load()
    hashtag_list = []
    for h in movies.select('title_hashtag').toPandas()['title_hashtag']: # get all documents
        hashtag_list.append(h)

    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)

    twitterStream = Stream(auth, listener())
    filterWords = hashtag_list
    twitterStream.filter(track=filterWords)
