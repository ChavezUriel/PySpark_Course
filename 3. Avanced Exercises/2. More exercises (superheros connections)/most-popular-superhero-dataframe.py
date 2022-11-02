from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel+Names")

lines = spark.read.text("Marvel+Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
# the first element in every row is the id of a superhero, the following ids are the linked superheros ids
# "id" column is created by splitting the "value" column (which is every line) and taking the first element
# then "connections" column is created by counting the number of elements - 1 of every row (minus 1 because the first element is the super hero id stored in "id" column)
# group by "id" column because there can be multiple rows for the same superhero ID. And we aggregate the grouped ids by summing the connections quantity
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# sort by connections column descending order and select the first only
mostPopular = connections.sort(func.col("connections").desc()).first()

# then we get the name using the id obtained using the names dataframe
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")


# Stop the session
spark.stop()
