from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# we define a similar function to the last one but changing the temperature from celsius to fahrenheit
# output format: (stationID, entryType, temperature)
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# parse de lines
lines = sc.textFile("1800.csv")
parsedLines = lines.map(parseLine)
# we take only the minimum temperatures by filtering the entryType
# the function used returns True for the rows we want to preserve
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# now all entryTypes in minTemps are indeed minimum temperatures 
# so we select the other two columns and put them in a new RDD
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# we use the stationID as key and summarize by taking the glonal minumum for that stationID 
# we should change "TMIN" for "TMAX"  in the minTemps RDD and chage here min() to max()
# to get the maximum temperatures instead
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# we collect the results and print them
results = minTemps.collect()

# now to

for result in results:
    # we formated to print it with F symbol
    print(result[0] + "\t{:.2f}F".format(result[1]))
