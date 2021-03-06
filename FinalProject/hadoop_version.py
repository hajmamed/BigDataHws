

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName('ReadTaxiTopic').getOrCreate()
sc = spark.sparkContext

print(f"Hadoop version = {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")