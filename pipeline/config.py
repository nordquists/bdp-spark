from pyspark.sql.types import *

TRAIN_WEEKS = 40
TARGET = 'SCORE'

# Defining schema for our time series data
schema_ts = StructType([
    StructField("REPO", StringType()),
    StructField("WEEK", IntegerType()),
    StructField("SCORE", LongType())
])
