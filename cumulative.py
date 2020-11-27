"""

Partition -> order by -> cumsum

"""
import sys
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
import pyspark.sql.functions as f


INPUT_DIR = "hdfs://dumbo/user/srn334/final/preprocessed_weekly"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/preprocessed_cumulative_weekly/"

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

sc.setLogLevel("WARN")

rdd = sc.textFile(INPUT_DIR)

ts = rdd.map(lambda line: line.split(",")).toDF(["repo", "week", "score"])

ts = ts.fillna({'score': 0, 'day': 0, 'repo': ''})


cum_sum = ts.withColumn('cumsum', f.sum(f.col("score")).over(Window.partitionBy('repo').orderBy(f.col("week")).rowsBetween(-sys.maxsize, 0)))
cum_sum.show(100)
