# it includes dataframes, which is the data structure everyone is trying to migrate
# We can run sql queries on dataframes
# and can be written easily into csv, json, hive, etc
# can connect to tableau, etc

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession (instead of spark context)
# we create or connect to a previous spark session
# note that we have to stop the session every time we run the script
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# this mapper function return Rows
def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

# we'll also have a spark context to try the possible interactions
lines = spark.sparkContext.textFile("fakefriends.csv")
# we map the rdd into an rdd containing row objects
people = lines.map(mapper)

# we create the dataframe from the people rdd (containing row objects)
schemaPeople = spark.createDataFrame(people).cache()
# create a temporary view of the dataframe (it enables us to use spark.sql method)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

# stop spark session
spark.stop()
