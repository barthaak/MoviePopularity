import requests
import findspark
import json
findspark.init()
from pymongo import MongoClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#movie = '{}movie/{}?api_key={}'.format(url,550,key)

def batch_job():

    key='233da043e658c47fd8661a48c2287fce'
    url = 'https://api.themoviedb.org/3/'

    my_spark = SparkSession.builder.appName("movieData").master("local").getOrCreate()
    sc = my_spark.sparkContext

    now_playing = '{}movie/{}?api_key={}'.format(url,"now_playing",key)
    response = requests.get(now_playing)
    out = response.json()
    out = [json.dumps(out)]

    rdd = sc.parallelize(out)
    data = my_spark.read.json(rdd)
    #data.write.mode("append").saveAsTable("movies")
    #my_spark.sql("SELECT results[1]['title'] FROM movies").show()

    client = MongoClient('localhost', 27017) # connect to the database server
    db= client.movieDatabase # get a database –alternative client['lab1db']
    movies = db.movies

    id_list = []
    for ids in movies.find({},{ "title": 0, "title_low":0, "title_hashtag":0 }): # get all documents
        id_list.append(list(ids.values())[0])

    pdData = data.toPandas()
    results = pdData['results'][0]
    new_id_list = []
    for v,w in enumerate(results):
        if results[v]['original_language']=='en':
            title = results[v]['title']
            title_low = title.lower()
            title_hashtag = ''.join(('#',title_low.replace(" ", "")))
            movie= {"_id": results[v]['id'], "title": title, "title_low":title_low, "title_hashtag":title_hashtag}
            movies.update_one({ "_id" : results[v]['id']},
                    { "$set": { "title": title, "title_low":title_low, "title_hashtag":title_hashtag }}, upsert=True
                )
            new_id_list.append(results[v]['id'])


    movies.delete_many( {'_id': { '$in': list( set(id_list) - set(new_id_list) ) } } )

    print(len(new_id_list))
