from flask import Flask, render_template, flash, redirect, url_for, session, request, logging, send_from_directory, Response
import os

#
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 --master local[2] pyspark-shell'

import findspark

findspark.init()

from pyspark.sql import SparkSession
mySpark = SparkSession.builder.appName("movieData").master("local").getOrCreate()
movies = mySpark.read.format("com.mongodb.spark.sql.DefaultSource") \
          .option("uri","mongodb://localhost:27017/movieDatabase.movies").load()
hashtag_list = []
for h in movies.select('title_hashtag').toPandas()['title_hashtag']: # get all documents
    hashtag_list.append(h)

movies_dict = {el:0 for el in hashtag_list}
#

from kafka import KafkaConsumer, TopicPartition
import schedule
import time

app = Flask(__name__)

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                     auto_offset_reset='earliest')
consumer.subscribe(['tweets_analysis'])

@app.route('/')
def index():
    return render_template('home.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    return Response(kafkastream(), mimetype='text/plain')
    #activate_stream_job().apply_async()

def kafkastream():
    for msg in consumer:
        result = ''
        movie_hashtag = msg.value.decode('utf-8').split(" ")[0]
        movie_count = int(msg.value.decode('utf-8').split(" ")[1])
        movies_dict[movie_hashtag] = movie_count

        sorted_movies = sorted(movies_dict.items(), key=lambda x: x[1], reverse=True)
        for x in sorted_movies:
            result += str(x[0]) + ' ' + str(x[1]) + '\n'

        result += '\n\n'
        yield(result)
        #yield(msg.value.decode('utf-8') + '\n')
        #yield(msg.value.decode('utf-8').split(" ")[0] + '\t' + msg.value.decode('utf-8').split(" ")[1] + '\n')

if __name__ == '__main__':
#     app.secret_key='secret123'
    app.run(debug=True)
