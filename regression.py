from pyspark import SparkContext
from pyspark.sql import HiveContext

INPUT_TABLE = "ts_monthly_preprocessed"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/preprocessed/"

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

sc.setLogLevel("WARN")


ts = hive_context.table("srn334.ts_day")
ts.registerTempTable('ts_day')

ts = hive_context.sql("SELECT * FROM {}".format(INPUT_TABLE))
