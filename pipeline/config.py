from pyspark.sql.types import *
from pyspark.ml.linalg import VectorUDT

TRAIN_WEEKS = 40
TARGET = 'score'

# Defining schema for our time series data
schema_ts = StructType([
    StructField("repo", StringType()),
    StructField("week", IntegerType()),
    StructField("features", VectorUDT()),
    StructField("score", DoubleType())
])
