from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel+Names")

lines = spark.read.text("Marvel+Graph")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))


# we reutilized the code from most-popular activity to import the data

# we filter to get the superheros having only 1 connection

oneConnectionIds = connections.filter(func.col("connections") == 1).select("id")

oneConnection = oneConnectionIds.join(names, "id")

print("The following characters have only 1 connection:")

oneConnection.select("name").show()

spark.stop()
