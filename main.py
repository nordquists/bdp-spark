import gc
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import *
from pipeline.config import TRAIN_WEEKS
from pipeline.features import apply_pipeline

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

# Register our time series data
ts = hive_context.table("srn334.ts_weekly")
ts.registerTempTable('ts_weekly')

ts = hive_context.sql("SELECT * FROM ts_weekly WHERE week <= {}".format(TRAIN_WEEKS))
# input = sc.textFile("hdfs://dumbo/user/srn334/final/indices")
#
# ts_df =

temp = apply_pipeline(ts)

ts.unpersist()
del ts
gc.collect()

# temp.createOrReplaceTempView("temp_table")
# hive_context.sql("create table feature_table as select * from temp_table")

result = temp.rdd\
    .map(tuple).map(lambda (repo, week, score, repo_indexed, repo_indexed_encoded, features): "{},{},{}".format(repo,str(features),str(score)))\
    .saveAsTextFile("hdfs://dumbo/user/srn334/final/test_output1/")
