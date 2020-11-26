from pyspark import SparkContext
import datetime

sc = SparkContext.getOrCreate()

output = sc.textFile("hdfs://dumbo/user/srn334/final/output/part-r-00000")
newdata = output.map(lambda line: line.split(","))

result = newdata.map(lambda row: ("{},{},{}," .format(row[0], row[1], datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S %Z').timetuple().tm_yday),row[4])).reduceByKey(lambda a, b: a + b)

result.saveAsTextFile("hdfs://dumbo/user/srn334/final/indexed_data_days/")
