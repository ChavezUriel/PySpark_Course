from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark =  SparkSession.builder.appName("TotalCustomerSpent").getOrCreate()

# creathe the schema
schema = StructType([ \
                     StructField("customerID", StringType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("spent", FloatType(), True)])


# reaad tge file as dataframe using the schema created
df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()

# select only customerID's and spents
customerSpent = df.select("customerID", "spent")
customerSpent.show()

# aggregate and find the sum of spents
spentAggregated = customerSpent.groupBy("customerID").sum("spent").withColumnRenamed("sum(spent)","total_spent").sort("total_spent")
spentAggregated.show()

# collect the results
results = spentAggregated.collect()

for result in results:
    print(result[0] + "\t{:.2f} $".format(result[1]))


spark.stop()