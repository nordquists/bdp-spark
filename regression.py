from pyspark import SparkContext
from pyspark.sql import HiveContext
import numpy as np
import pyspark.sql.functions as f


TRAINING_CUT_OFF = 9
INPUT_TABLE = "ts_weekly_preprocessed"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/regression{}/".format(str(TRAINING_CUT_OFF))

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
sc.setLogLevel("OFF")


class LinearRegression:
    def __init__(self):
        self.__slope = None
        self.__intercept = None
        self.__r_squared = None
        self.__p_value = None
        self.__std_err = None
        self.__fitted = None

    def fit(self, x, y, order=1):
        self.__fitted = np.polyfit(x, y, order)
        self.__slope = self.__fitted[0]
        self.__intercept = self.__fitted[1]

    def predict(self, x):
        if self.__fitted is None:
            return None

        predictions = np.polyval(self.__fitted, x)

        return predictions

    def evaluate(self, predictions, y):
        abs_error = predictions - y
        squared_error = np.square(abs_error)
        mean_squared_error = np.mean(squared_error)
        rmse = np.sqrt(mean_squared_error)
        r2 = 1.0 - (np.var(abs_error) / np.var(y))

        return rmse, r2

    def get_slope(self):
        return self.__slope

    def get_intercept(self):
        return self.__intercept


def map_linear_regression(line):
    # # We have to
    # repo_ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
    # repo_ts.registerTempTable('{}'.format(INPUT_TABLE))
    #
    # repo_ts = hive_context.sql("SELECT * FROM ts_monthly_preprocessed where repo = '" +  repo_name + "'")
    repo_name, x, y = line

    x = np.array(x)
    y = np.array(y)

    lr = LinearRegression()

    lr.fit(x, y)
    predictions = lr.predict(x)
    rmse, r2 = lr.evaluate(predictions, y)
    slope = lr.get_slope()
    intercept = lr.get_intercept()

    return "{},{},{},{},{},{}".format(repo_name, slope, intercept, r2, len(x), max(y))
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

ts = hive_context.sql("SELECT * FROM {} WHERE week < {}".format(INPUT_TABLE, str(TRAINING_CUT_OFF)))

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

df = ts.groupBy("repo").agg(f.collect_list("week"), f.collect_list("score"))

result = df.rdd.map(tuple).map(map_linear_regression)

print("STARTING REGRESSION MAP: --------------------------------")
# print(result.take(10))

#
# print(OUTPUT_DIR)
#
result.saveAsTextFile(OUTPUT_DIR)















