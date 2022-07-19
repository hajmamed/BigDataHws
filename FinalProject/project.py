import datetime
import sys

import pyspark
from pyspark.sql import SparkSession
filePath = 'data.csv'

spark = SparkSession.builder.appName('BigDataHw4').getOrCreate()
sc=spark.sparkContext
df=spark.read.format("csv").option("header","true").load(filePath)
df.printSchema()




date_time_str = '2018-06-29 08:15:27.243860'
date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')

print('Date:', date_time_obj.date())
print('Time:', date_time_obj.time())
print('Date-time:', date_time_obj)
timestamp = datetime.datetime.timestamp(date_time_obj)
print(timestamp)

from kafka import KafkaProducer, KafkaConsumer
import json
from json import loads
from csv import DictReader

counter = 1
with open('data.csv', 'r') as new_obj:
    csv_dr = DictReader(new_obj)
    for row in csv_dr:
        row['uuid'] = counter
        counter += 1
        print(row)
        a = json.dumps(row)
        print('asghar')

bootstrap_servers = ['localhost:9092']
topic_name = 'taxi-topic'
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))
try:
    for message in consumer:
        print(message.value)
except KeyboardInterrupt:
    sys.exit()