from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("Book")
# we use flat map to get an RDD of all the words
words = input.flatMap(lambda x: x.split())
# then we count the freuency of every word
wordCounts = words.countByValue()

# we print the result
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
