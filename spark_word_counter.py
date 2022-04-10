import re
import sys
from operator import add
from pyspark import SparkContext

WORD_RE = re.compile(r"[\w']+")


def main():
    # read in input, output path
    args = sys.argv[1:]
    if len(args) != 2:
        raise ValueError

    inputPath, outputPath = args
    sc = SparkContext(appName='Hamidi Spark wordcount script')
    lines = sc.textFile(inputPath)
    # lines.flatMap(WORD_RE.findall) doesn't work on Spark 1.6.2; apparently
    # it can't serialize instance methods?
    counts = (
        lines.flatMap(lambda line: WORD_RE.findall(line))
            .map(lambda word: (word, 1))
            .reduceByKey(add))
    counts.saveAsTextFile(outputPath)
    output = counts.collect()
    #for (word, count) in output:
    #    print("%s: %i" % (word, count))

    sc.stop()

if __name__ == '__main__':
    main()
