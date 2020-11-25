from pyspark import SparkContext

sc = SparkContext.getOrCreate()

input = sc.textFile("hdfs://dumbo/user/srn334/final/indices")

newdata = input.map(lambda line: line.split(","))


result = newdata.map(mapFunc).reduceByKey(lambda a, b: a + b)

result.saveAsTextFile("hdfs://dumbo/user/srn334/final/split/")