from pyspark.sql.types import *

TRAIN_WEEKS = 40
TARGET = 'score'

# Defining schema for our time series data
schema_ts = StructType([
    StructField("repo", StringType()),
    StructField("week", IntegerType()),
    StructField("features", ArrayType(StructType([
        StructField("week", DoubleType()),
        StructField("week", DoubleType()),
        StructField("week", DoubleType())
    ]))),
    StructField("score", DoubleType())
])
