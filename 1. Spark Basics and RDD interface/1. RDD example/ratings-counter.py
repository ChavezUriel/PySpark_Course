# to create rdd's we need spark context and to create it we need spark conf (to configure the context)
# conf can configure where we want to run our code
from pyspark import SparkConf, SparkContext
# standard python import (for sorting final resutls)
import collections

### Create context
# we use conf to set master to "local" (local box, not a cluster, not multiple cpu's)
# later we will configure to use every core of the procesor
# finally we wet the app name to "RatingsHistogram"
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# using this conficuration we create the context
sc = SparkContext(conf = conf)

### Create the rdd
# load our data file
# the lines variable has as values all the rows in the data
lines = sc.textFile("file:///Users/tu_rk/Desktop/GitCourses/Spark_Course/ml-100k/u.data")

### Map the rdd (take a column)
# then we take the ratings column which is the third one
# every row separates the columns with spaces " " chars,
# thats why we use the split function and take the second element
# this function returns another rdd which is stored in "ratings"
ratings = lines.map(lambda x: x.split()[2])

### Count on an rdd
# finally we count the frecuency of every value in the single column rdd we created
# this returns a simple python object, not an rdd, so we can use it as usual python
result = ratings.countByValue()

### Sort and print results
# with standard python functions we sort the the results and put them into a dictionary
sortedResults = collections.OrderedDict(sorted(result.items()))
# finally we print them
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
