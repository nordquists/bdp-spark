from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import *
from pipeline.config import schema_ts
from pipeline.features import apply_pipeline

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

# Register our time series data
ts = hive_context.table("srn334.ts_weekly")
ts.registerTempTable('ts_weekly')

ts_df = hive_context.sql("SELECT * FROM ts_weekly LIMIT 1000") # , schema=schema_ts

transformed_data = apply_pipeline(ts_df)

transformed_data.show(20)

# rdd = transformed_data.rdd.map(tuple)
#
# result = rdd.map(lambda (repo, week, score, repo_indexed, repo_indexed_encoded, features): "{},{},{}".format(repo,str(features),str(score)))
#
# # print(result)
#
# result.saveAsTextFile("hdfs://dumbo/user/srn334/final/test_output/")