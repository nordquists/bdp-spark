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

original_df = input.map(lambda line: line.split(",")).toDF("repo", "week", "score")

sum_df = input.map(lambda line: line.split(",")).map(lambda (x, y, z): (x, int(z))).reduceByKey(lambda a, b: a + b).toDF("repo", "score")


original_df.show(5)

sum_df.show(6)


sum_df = sum_df.filter(f.col('score') > 10000)

sum_df.show(7)

result_df = original_df.join(sum_df, ["repo"], "left_semi")

result_df.show(8)

result_df.rdd\
    .map(tuple).map(lambda (repo, week, score): "{},{},{}".format(repo,week,str(score)))\
    .saveAsTextFile("hdfs://dumbo/user/srn334/final/filtered/")
