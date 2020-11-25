from pyspark.sql.types import *

TRAIN_WEEKS = 40
TARGET = 'SCORE'

# Defining schema for our time series data
schema_ts = StructType([
    StructField("repo", StringType()),
    StructField("week", IntegerType()),
    StructField("score", LongType())
])
