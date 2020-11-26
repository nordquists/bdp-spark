from pyspark.sql.types import *

TRAIN_WEEKS = 40
TARGET = 'score'

# Defining schema for our time series data
schema_ts = StructType([
    StructField("repo", StringType()),
    StructField("features", IntegerType()),
    StructField("score", DoubleType())
])
