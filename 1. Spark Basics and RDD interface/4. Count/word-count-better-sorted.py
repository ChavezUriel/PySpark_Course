import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("Book")
words = input.flatMap(normalizeWords)

# we will count on a different way, bay adding a column we can count with reduceByKey
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# now we flip key <-> values and sort by key (sort by frecuency of each word)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
