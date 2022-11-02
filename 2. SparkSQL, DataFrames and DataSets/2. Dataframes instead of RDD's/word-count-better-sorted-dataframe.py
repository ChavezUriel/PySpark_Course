from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
# we can read txt and store it in dataframes
# even when it isnt structured like plain text
# every row has a single column named value with the text in it
inputDF = spark.read.text("book.txt")

# Split using a regular expression that extracts words
# the function split splits the words of every row 
# and then the function explode creates a new word for every element (word) given by split funciton
# we use the alias "word" to create the column named like that
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
# we clean data by deleting empty words
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())
