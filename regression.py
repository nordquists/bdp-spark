from pyspark import SparkContext
from pyspark.sql import HiveContext
from regression.linear import LinearRegression
import numpy as np
import pyspark.sql.functions as f


TRAINING_CUT_OFF = 9
INPUT_TABLE = "ts_monthly_preprocessed"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/regression{}/".format(str(TRAINING_CUT_OFF))

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
# sc.setLogLevel("OFF")

def map_linear_regression(repo_name):
    # # We have to
    # repo_ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
    # repo_ts.registerTempTable('{}'.format(INPUT_TABLE))
    #
    repo_ts = hive_context.sql("SELECT * FROM ts_monthly_preprocessed where repo = '" +  repo_name + "'")

    lr = LinearRegression()

    x = np.array(ts.select('day').collect()).flatten()
    y = np.array(ts.select('score').collect()).flatten()

    lr.fit(x, y)
    predictions = lr.predict(x)
    rmse, r2 = lr.evaluate(predictions, y)
    slope = lr.get_slope()
    intercept = lr.get_intercept()

    return "{},{},{},{}".format(repo_name, slope, intercept, r2)
    # repo_ts.show(10)
    #
    # return "{},1".format(repo_name)

def do_regression(repos):
    print("STARTING REGRESSION MAP: --------------------------------")
    result = repos.map(map_linear_regression)
    print("FINISHED REGRESSION MAP: --------------------------------")
    return result


ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
ts.registerTempTable('{}'.format(INPUT_TABLE))

ts = hive_context.sql("SELECT * FROM {} WHERE month < {}".format(INPUT_TABLE, str(TRAINING_CUT_OFF)))

# # First we find all of the repositories that we will run a regression on.
# print("LOADING REPOSITORIES: ------------------------------------")
# repos = ts.rdd.map(tuple).map(lambda (x, y, z): x)
# print(repos.take(100))
# print("LOADED {} REPOSITORIES: -----------------------------".format(repos.count()))
#
#
# to_process = repos.collect()
#
# for repo in to_process:
#
#
# print("STARTING REGRESSION MAP: --------------------------------")
# result = repos.map(map_linear_regression)
# print("FINISHED REGRESSION MAP: --------------------------------")
#
# print(result.take(100))


entries = ts.select("repo").distinct

df = ts.groupBy("repo").agg(f.collect_list("week"), f.collect_list("score"))

df.show(100)


#
# print(OUTPUT_DIR)
#
# result.saveAsTextFile(OUTPUT_DIR)















