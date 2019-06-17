from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("CustomerTotalSpent")
sc = SparkContext(conf = conf)
sc.setLogLevel("WARN")

def parseLine(line):
    fields = line.split(',')
    customer = fields[0]
    amount = fields[2]
    return (int(customer),float(amount))

lines = sc.textFile("/Users/vanka/Documents/SparkCourse/customer-orders.csv")
customer_data = lines.map(parseLine)
customer_total = customer_data.reduceByKey(lambda x, y: x + y).sortByKey()
results = customer_total.collect()

for result in results:
    print(result)
