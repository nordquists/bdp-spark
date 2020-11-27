from pyspark import SparkContext
from pyspark.sql import HiveContext
import numpy as np
import pyspark.sql.functions as f

INPUT_TABLE = "week_regression_cumulative"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/final_index/"

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
sc.setLogLevel("OFF")



def index_mapper(line):
    repo_name, slope, intercept, r2, entries, integral, derivative = line

    index = (0.4 * integral + 0.6 * derivative)
    index *= entries / 52
    if integral:
        index += derivative / integral

    return "{},{}".format(repo_name, index)



ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
ts.registerTempTable('{}'.format(INPUT_TABLE))

ts = hive_context.sql("SELECT * FROM {}".format(INPUT_TABLE))


result = ts.rdd.map(tuple).map(index_mapper)

result = result.sortBy(lambda a: a[2])

result.saveAsTextFile(OUTPUT_DIR)
