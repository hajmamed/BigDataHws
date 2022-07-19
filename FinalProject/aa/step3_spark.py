topicname="spark-data-in"
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import os
os.environ['PYSPARK_SUBMIT_ARGS']="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell"
kafka_boot_server="localhost:9092"
spark=SparkSession.builder.appName("streaming spark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df=spark.readStream.format("kafka").option("kafka.bootstrap.servers",kafka_boot_server).option("subscribe",topicname).load()
DF=df.selectExpr("CAST(value as STRING)")

#import pyspark.sql.functions as F
#import pyspark.sql.types as T
#my_udf=F.udf(lambda x : x.encode().decode('unicode-escape'),T.StrringType())
#Df=df.withColumn('Value',my_udf('value'))

from pyspark.sql import functions as F
DF=DF.select('value',F.split('value', ',').alias('new_value'))
DF=DF.select('value',*[DF['new_value'][i] for i in [0,1,2,3,4]])
DF=DF.select('value',F.split('new_value[0]',': ').alias('Date/Time'),F.split('new_value[1]',':').alias('Lat'),F.split('new_value[2]',':').alias('Lon'),F.split('new_value[3]',':').alias('Base'),F.split('new_value[4]',':').alias('UUID'))

DF=DF.select('value',*[DF['Date/Time'][1]],*[DF['Lat'][1]],*[DF['Lon'][1]],*[DF['Base'][1]],*[DF['UUID'][1]])
DF=DF.withColumnRenamed("Lat[1]","Lat")
DF=DF.withColumnRenamed("Lon[1]","Lon")
DF=DF.withColumnRenamed("Base[1]","Base")
DF=DF.withColumnRenamed("UUID[1]","UUID")
DF=DF.withColumnRenamed("Date/Time[1]","Date/Time")
lst=['"','}']
#import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
#myudf=F.udf(lambda 
myudf=udf(lambda x:x.replace('"',""))
myudf1=udf(lambda x:x.replace('}',""))
DF=DF.withColumn('Date/Time',myudf(col('Date/Time')))
DF=DF.withColumn('Base',myudf(col('Base')))
DF=DF.withColumn('Lat',myudf(col('Lat')))
DF=DF.withColumn('Lon',myudf(col('Lon')))
DF=DF.withColumn('UUID',myudf1(col('UUID')))
#DF=DF.withColumn("Base",lambda x:''.join([i for i in x.split() if i not in lst]))
DF=DF.withColumn("Lat",DF.Lat.cast('double'))
DF=DF.withColumn("Lon",DF.Lon.cast('double'))
DF=DF.withColumn("UUID",DF.UUID.cast('integer'))
#DF=DF.withColumn("Lon",lambda x:''.join([i for i in x.split() if i not in lst]),IntegerType)
#DF=DF.withColumn("UUID",lambda x:''.join([i for i in x.split() if i not in lst]),IntegerType)

from pyspark.ml.clustering import KMeansModel
model=KMeansModel.load("/home/alireza/Desktop/clustering_model_new")
print("model has been loaded successfully")
from pyspark.ml.feature import *
hasher=FeatureHasher(inputCols=["Lat","Lon","Base"],outputCol="features")
DF=hasher.transform(DF)
from pyspark.ml.clustering import KMeans
DF=model.transform(DF)
topic1="data-out"
#.option("topic", topic1)
#.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
df.printSchema()
DF.printSchema()
#writeStream
#    .format("kafka")
#    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
#    .option("topic", "updates")
#    .start()

#  .trigger(continuous="1 second") \     # only change in query
#    .option("checkpointLocation", "path/to/checkpoint/dir") \
#query=DF.writeStream.format("kafka").option("checkpointLocation", "/home/alireza/ckp").option("kafka.bootstrap.servers","localhost:9092").option("topic", topic1).start() # Append instead of update
#query=DF.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option('subscribe',"data-out").start() # Append instead of update
query=DF.writeStream.trigger(processingTime='5 seconds').outputMode("update").format("console").start() # Append instead of update
query.awaitTermination()
