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

ts_df = hive_context.sql("SELECT * FROM ts_weekly") # , schema=schema_ts

transformed_data = apply_pipeline(ts_df)

transformed_data.show(20)