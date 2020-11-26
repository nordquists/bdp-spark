from pyspark import SparkContext
from pyspark.sql import HiveContext

INPUT_TABLE = "ts_monthly_preprocessed"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/preprocessed/"

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

sc.setLogLevel("WARN")

ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
ts.registerTempTable('{}'.format(INPUT_TABLE))

ts = hive_context.sql("SELECT * FROM {}".format(INPUT_TABLE))

# First we find all of the repositories that we will run a regression on.
repos = ts.rdd.map(lambda (x, y, z): x)




