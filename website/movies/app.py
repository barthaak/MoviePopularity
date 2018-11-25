from flask import Flask, render_template, flash, redirect, url_for, session, request, logging, send_from_directory, Response
import os

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
        yield(msg.value.decode('utf-8') + '\n')
        #yield(msg.value.decode('utf-8').split(" ")[0] + '\t' + msg.value.decode('utf-8').split(" ")[1] + '\n')

if __name__ == '__main__':
#     app.secret_key='secret123'
    app.run(debug=True)
