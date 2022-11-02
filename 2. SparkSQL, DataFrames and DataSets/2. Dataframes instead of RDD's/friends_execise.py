from pyspark.sql import SparkSession

from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsExercise").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

# group by age and compute the average friends
print("Raw version:")
people.groupBy("age").avg("friends").show()

# sorting by age
print("Sorted:")
people.groupBy("age").avg("friends").sort("age").show()

# formatting via aggregation method (agg)
print("Rounding via agg method:")
people.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()

# custom column name
print("Adding custom name to column:")
people.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

# USING SQL
# create temp view of the dataframe so we can acces it using sql
people.createOrReplaceTempView("people")
print("Using sql:")
spark.sql("SELECT age, ROUND(AVG(friends),2) AS avg_friends FROM people GROUP BY age ORDER BY age ASC").show()

spark.stop()
