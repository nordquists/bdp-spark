from pyspark import SparkContext
from pyspark.sql import HiveContext
from regression.linear import LinearRegression
import numpy as np

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










def do_regression(repos):




def map_linear_regression(repo_name):
    global hive_context
    global INPUT_TABLE

    # We have to
    ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
    ts.registerTempTable('{}'.format(INPUT_TABLE))

    ts = hive_context.sql("SELECT * FROM {} where repo = '{}'".format(INPUT_TABLE, repo_name))

    lr = LinearRegression()

    x = np.array(ts.select('day').collect()).flatten()
    y = np.array(ts.select('score').collect()).flatten()

    predictions = lr.predict(x)
    rmse, r2 = lr.evaluate(predictions, y)
    slope = lr.get_slope()
    intercept = lr.get_intercept()

    return "{},{}".format(repo_name, slope, intercept, r2)







