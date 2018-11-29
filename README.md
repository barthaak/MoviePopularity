# Movie Popularity Analyzer
A project encorporating big data technologies in order to stream and analyze twitter content related to movies.

## Prerequisites

What software you need to run this application

* [Python 3](https://www.python.org/)
* [Flask](http://flask.pocoo.org/) - Web Framework
* [MongoDB](https://www.mongodb.com/) - Database
* [Apache Spark](https://spark.apache.org/) - Data Processing
* [Kafka](https://kafka.apache.org/) - Message Broker

## Running the application

* When running the app for the first time:
    * Execute Movie Data Batch notebook once.
    * Start zookeeper server.
    * Start kafka server.
    * Create topics "tweets" and "tweets_analysis"
    
* If not running the app for the first time there:
    * Start zookeeper server.
    * Start kafka server.
    * Execute Twitter notebook (gets tweets and puts them in tweets topic).
    * Execute Filter Tweets notebook (gets tweets from tweets topic, filters and counts and puts the results in tweets_analysis topic).
    * run app.py to start the website.
    * go to http://localhost:5000 -> dashboard to view the live stream.


## Pipeline and files explanation
1. Retrieve all movies currently playing, this is a batch operation and can be run once a day
1. File: Movie Data Batch.ipynb

2. Get Tweets with hastags related to the movies
2. File: Twitter.ipynb

3. Process and analyze the tweets
3. File: Filter Tweets.ipynb

(Optional)
4. Visualize results with notebook 
4. File: Visualize.ipynb

5. Get Results from kafka topic and display updates
5. Website folder (flask app), app.py contains all logic
