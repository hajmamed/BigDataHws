from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, IntegerType
from elasticsearch import Elasticsearch
import os
import datetime
import pickle

model_filename = 'finalized_model'

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

    # load the model from disk
    loaded_model = pickle.load(open(model_filename, 'rb'))
    y_pred = loaded_model.predict(row)
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
    bootstrap_servers = ['localhost:9092']
    topic_name = 'taxi_topic'
    # consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, api_version=(0, 10, 1))
    # for msg in consumer:
    #     print(msg)
    

    # ignore 404 and 400
    es.indices.delete(index='uber_data', ignore=[400, 404])

    es.indices.create(index='uber_data', mappings=mappings, ignore=[400])

    spark = SparkSession.builder.master("local").appName('ReadTaxiTopic').getOrCreate()
    sc = spark.sparkContext

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "taxi_topic") \
        .load()
    df.printSchema()

    # .option("startingOffsets", "earliest") \
    #     .option("endingOffsets", "latest") \
    #     .option("includeHeaders	", "true") \
    # query = df.writeStream.foreach(process_row).start()

    ds = df.selectExpr("CAST(value AS STRING)")
    rawQuery = df \
        .writeStream \
        .queryName("qraw") \
        .format("memory") \
        .start()
    pointsQuery = ds \
        .writeStream \
        .queryName("qpoints") \
        .format("memory") \
        .start()
    raw = spark.sql("select * from qraw")
    raw.show()

    points = spark.sql("select * from qpoints")
    points.show()
    
    counter = 1
    for index, point in points.iterrows():
        print("Process points " + str(counter) + "/" + str(len(points)), end="\r")
        process_row(counter, point)
        counter += 1

    # schema = StructType().add("Date/Time", StringType()).add("Lat", IntegerType()).add("Lon", IntegerType())
    # df.select(col("key").cast("string"), from_json(col("value").cast("string"), schema))
    # query = df.writeStream.foreach(process_row).start()
    print("after query")


consume_taxi_topic()

# spark-submit --deploy-mode client --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 .\consumer.py
