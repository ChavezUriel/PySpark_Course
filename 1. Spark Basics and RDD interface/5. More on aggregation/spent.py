from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

# parse function
def parseLine(line):
  elements = line.split(",")
  customerID = elements[0]
  spent = elements[2]
  return (customerID, spent) 

data = sc.textFile("customer-orders.csv")
customerSpent = data.map(parseLine)
# we sum over the customerID to get the total spent
customerTotal = customerSpent.reduceByKey(lambda x,y: float(x) + float(y))
# sort
customerTotalSorted = customerTotal.map(lambda x: (x[1],x[0])).sortByKey()
results = customerTotalSorted.collect()

for totalSpent, customerID in results:
  print(customerID + ": " + str(round(totalSpent,2)))
  




