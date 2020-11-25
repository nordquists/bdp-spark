from datetime import datetime

from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType

from sparkts.datetimeindex import uniform, BusinessDayFrequency
from sparkts.timeseriesrdd import time_series_rdd_from_observations

def loadObservations(sparkContext, sqlContext, path):
    textFile = sparkContext.textFile(path)
    rowRdd = textFile.map(lineToRow)
    schema = StructType([
        StructField('timestamp', TimestampType(), nullable=True),
        StructField('name', StringType(), nullable=True),
        StructField('score', DoubleType(), nullable=True),
    ])
    return sqlContext.createDataFrame(rowRdd, schema)

if __name__ == "__main__":
    sc = SparkContext(appName="Stocks")
    sqlContext = SQLContext(sc)

    tickerObs = loadObservations(sc, sqlContext, "hdfs://dumbo/user/srn334/final/indexed_data")