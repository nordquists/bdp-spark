from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import *

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

# Defining schema for our time series data
schema_ts = StructType([
    StructField("REPO", StringType()),
    StructField("WEEK", IntegerType()),
    StructField("SCORE", LongType())
])

# Defining schema for our dev set data
schema_dev_set = StructType([
    StructField("INDEX", IntegerType()),
    StructField("REPO", StringType())
])

# Register our dev set
ts = hive_context.table("srn334.dev_set")
ts.registerTempTable('dev_set')

# Getting the repo names in our dev set
repo_names_dev_df = hive_context.sql("SELECT DISTINCT repo FROM dev_set", schema=schema_dev_set)

# Register our time series data
ts = hive_context.table("srn334.ts")
ts.registerTempTable('ts')

# Pulling in our time series data
data_df = hive_context.sql("SELECT DISTINCT repo FROM dev_set", schema=schema_ts)

# We only want to work with data in our dev set, so we do a left semi join
dev_data_df = data_df.join(repo_names_dev_df, "REPO", 'left_semi')