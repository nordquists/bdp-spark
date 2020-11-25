from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import *
from pipeline.config import TRAIN_WEEKS, schema_ts
from pipeline.features import apply_pipeline

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

# Register our time series data
ts = hive_context.table("srn334.ts_weekly")
ts.registerTempTable('ts_weekly')

# ts_df = hive_context.sql("SELECT * FROM ts_weekly WHERE week <= {}".format(TRAIN_WEEKS))
input = sc.textFile("hdfs://dumbo/user/srn334/final/indices")

input = input.map(lambda line: line.split(",")).map(lambda (repo, week, score): (repo, int(week), int(score)))

ts_df = hive_context.createDataFrame(input, schema=schema_ts)

# ts_df.show(20)

transformed_data = apply_pipeline(ts_df)

result = transformed_data.rdd\
    .map(tuple).map(lambda (repo, week, score, repo_indexed, repo_indexed_encoded, features): "{},{},{}".format(repo,str(features),str(score)))\
    .saveAsTextFile("hdfs://dumbo/user/srn334/final/test_output/")
