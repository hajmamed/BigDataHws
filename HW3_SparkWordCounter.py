import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName('HamidiSparkWordCount').getOrCreate()
sc = spark.sparkContext
# create Spark context with necessary configuration
# sc = SparkContext("local","PySpark Word Count Exmaple")

# read data from text file and split each line into words
words = sc.textFile("testfile.txt").flatMap(lambda line: line.split(" "))

# count the occurrence of each word
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# save the counts to output
wordCounts.saveAsTextFile("output")
#
#
#
#
#
#
#
# import sys
#
# from pyspark import SparkContext, SparkConf
#
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName('HamidiSparkWordCount').getOrCreate()
# sc=spark.sparkContext
#
# # create Spark context with Spark configuration
# # conf = SparkConf().setAppName("SparkWordCount")
# # sc = SparkContext(conf=conf)
#
# # get threshold
# # threshold = int(sys.argv[2])
# threshold = 1
#
# # read in text file and split each document into words
# # tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))
# tokenized = sc.textFile("testfile.txt").flatMap(lambda line: line.split(" "))
#
# # count the occurrence of each word
# wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
#
# # filter out words with fewer than threshold occurrences
# filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)
#
# # count characters
# charCounts = filtered.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2)
#
# list = charCounts.collect()
# print(repr(list)[1:-1])
