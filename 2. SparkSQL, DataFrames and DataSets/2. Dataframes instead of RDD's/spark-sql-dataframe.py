# now we'll use a csv file with heather in it
from pyspark.sql import SparkSession

# session creation
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# we select in the option method "heather", "true"
# we select inferschema
# we are using only the spark session, not the context
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")

# print the schema
print("Here is our inferred schema:")
people.printSchema()

# print some examples:

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()

