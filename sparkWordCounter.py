from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master("local")\
                    .appName('HamidiSparkWordCount')\
                    .getOrCreate()
sc=spark.sparkContext

# Read the input file and Calculating words count
text_file = sc.textFile("testfile.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

sc.stop()
spark.stop()
