from pyspark import SparkContext
from pyspark.sql import HiveContext
from regression.linear import LinearRegression
import numpy as np

TRAINING_CUT_OFF = 9
INPUT_TABLE = "ts_monthly_preprocessed"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/regression{}/".format(str(TRAINING_CUT_OFF))

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
sc.setLogLevel("OFF")

def map_linear_regression(repo_name):
    print(repo_name)
    # global hive_context
    # global INPUT_TABLE
    #
    # # # We have to
    # # ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
    # # ts.registerTempTable('{}'.format(INPUT_TABLE))
    #
    # ts = hive_context.sql("SELECT * FROM {} where repo = '{}'".format(INPUT_TABLE, repo_name))
    #
    # lr = LinearRegression()
    #
    # x = np.array(ts.select('day').collect()).flatten()
    # y = np.array(ts.select('score').collect()).flatten()
    #
    # predictions = lr.predict(x)
    # rmse, r2 = lr.evaluate(predictions, y)
    # slope = lr.get_slope()
    # intercept = lr.get_intercept()
    #
    # return "{},{},{},{}".format(repo_name, slope, intercept, r2)
    return ""

def do_regression(repos):
    print("STARTING REGRESSION MAP: --------------------------------")
    result = repos.map(map_linear_regression)
    print("FINISHED REGRESSION MAP: --------------------------------")
    return result


ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
ts.registerTempTable('{}'.format(INPUT_TABLE))

ts = hive_context.sql("SELECT * FROM {} WHERE month < {}".format(INPUT_TABLE, str(TRAINING_CUT_OFF)))

# First we find all of the repositories that we will run a regression on.
print("LOADING REPOSITORIES: ------------------------------------")
repos = ts.rdd.map(tuple).map(lambda (x, y, z): x)
print(repos.take(100))
print("LOADED {} REPOSITORIES: -----------------------------".format(repos.count()))

result = repos.map(map_linear_regression)
#
# print(OUTPUT_DIR)
#
# result.saveAsTextFile(OUTPUT_DIR)















