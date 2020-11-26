"""

We need to reduce the size of our dataset so we can train on it, otherwise we will not be able to even load all the data
into memory.

"""

from pyspark.sql.types import *

from pyspark import SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as f

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

input = sc.textFile("hdfs://dumbo/user/srn334/final/indices/")

newdata = input.map(lambda line: line.split(","))

result = newdata.map(lambda (x, y, z): (x, int(z))).reduceByKey(lambda a, b: a + b)

schema_ts = StructType([
    StructField("repo", StringType()),
    StructField("week", IntegerType()),
    StructField("score", DoubleType())
])

original_df = hive_context.createDataFrame(newdata, schema=schema_ts)

schema_ts = StructType([
    StructField("repo", StringType()),
    StructField("score", DoubleType())
])

sum_df = hive_context.createDataFrame(result, schema=schema_ts)

sum_df = sum_df.filter(f.col('score') > 10000)

result_df = original_df.join(sum_df, ["repo"], "left_semi")

result_df.rdd\
    .map(tuple).map(lambda (repo, week, score): "{},{},{}".format(repo,week,str(score)))\
    .saveAsTextFile("hdfs://dumbo/user/srn334/final/filtered/")
