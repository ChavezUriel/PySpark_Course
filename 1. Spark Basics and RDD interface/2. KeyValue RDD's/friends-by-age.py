# Imports
from pyspark import SparkConf, SparkContext

# spark context creation
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# define a fucntion to parse every line
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# create rdd
lines = sc.textFile("fakefriends.csv")
# parse the lines with the function we created
# the rdd returned has 2 columns, it can be treated as key-value RDD
rdd = lines.map(parseLine)
# we first take the values column (numFriends) and transfor it to a list (numFriends, 1)
# so every element on the rdd has this structure: age, (numFriends, 1)
# then, when using reduceByKey we summarize the numfriends and the 1 by column
# so we have the next structure: age(unique), (sum of friends, count)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# with the count value we can then compute the average by dividing
# so the structure now is: age(unique), average num of friends
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# we sort the results by key (by age)
averagesByAge = averagesByAge.sortByKey()
# We finally collect the results into an array and print it
# (collect() is the function that indicates spark to return the RDD to python)
results = averagesByAge.collect()
for result in results:
    print(result[0], round(result[1],2))
