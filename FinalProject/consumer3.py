import datetime

from kafka import KafkaConsumer
import os
from elasticsearch import Elasticsearch
import pandas as pd
from pyspark.ml.clustering import KMeansModel

model_filename = "finalized_model"

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 pyspark-shell'

es = Elasticsearch(host='localhost', port='9200')

mappings = {
    "properties": {
        "Date/Time": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "Lat": {
            "type": "float"
        },
        "Lon": {
            "type": "float"
        },
        "date": {
            "type": "date",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "day": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "location": {
            "type": "geo_point",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "time": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "timestamp": {
            "type": "date"
        },
        "uuid": {
            "type": "long",
        }
    }
}

best_clusters_count = 4

def removeStopWords(text):
    t = ''
    tokens = es.indices.analyze(body={
        "analyzer": 'stop',
        "text": text
    })['tokens']
    for token in tokens:
        t += token['token'] + " "
    return t


def get_value(row):
    return row["value"]


def get_key(row):
    return row["key"]


def process_row(counter, row):
    dt = datetime.datetime.strptime(row['Date/Time'], '%m/%d/%Y %H:%M:%S')
    timestamp = int(datetime.datetime.timestamp(dt))

    kmeans = KMeansModel.load(model_filename)

    a = [row.Lat, row.Lon]
    y_pred = kmeans.predict([a, [0, 1]])

    print(y_pred)

    point_prop = {
        'uuid': str(counter),
        'Lat': row['Lat'],
        'Lon': row['Lon'],
        'Date/Time': row['Date/Time'],
        'date': str(dt.date()),
        'day': str(dt.day),
        'time': str(dt.time().isoformat()),
        'timestamp': timestamp,
        'location': str(row['Lon']) + "," + str(row['Lat']),
    }
    r = es.index(index='uber_data', id=str(counter), document=point_prop)
    # print("point " + str(counter) + " added")
    return r


def consume_taxi_topic():
    points = pd.read_csv('data.csv')

    # ignore 404 and 400
    es.indices.delete(index='uber_data', ignore=[400, 404])

    es.indices.create(index='uber_data', mappings=mappings, ignore=[400])
    counter = 1
    for index, point in points.iterrows():
        print("Process points " + str(counter) + "/" + str(len(points)), end="\r")
        process_row(counter, point)
        counter += 1
    print("after query")


consume_taxi_topic()

# spark-submit --deploy-mode client --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 .\consumer.py
