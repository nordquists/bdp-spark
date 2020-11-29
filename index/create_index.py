from index.linear_regression import LinearRegression
from pyspark import SparkContext
from pyspark.sql import HiveContext
import numpy as np
import pyspark.sql.functions as f

TRAINING_CUT_OFF = 45
INPUT_TABLE = "weekly_cumulative"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/final_index/"

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
sc.setLogLevel("OFF")


def map_linear_regression_derivative(line):
    # # We have to
    # repo_ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
    # repo_ts.registerTempTable('{}'.format(INPUT_TABLE))
    #
    # repo_ts = hive_context.sql("SELECT * FROM ts_monthly_preprocessed where repo = '" +  repo_name + "'")
    repo_name, x, cumsum = line

    x = np.array(x)
    y = np.array(cumsum)

    lr = LinearRegression()

    lr.fit(x, y)
    predictions = lr.predict(x)
    rmse, r2 = lr.evaluate(predictions, y)
    slope = lr.get_slope()
    intercept = lr.get_intercept()

    if len(cumsum) > 2:
        dydx = (cumsum[-1] - cumsum[0]) / (len(x) * 7)
    else:
        dydx = slope

    return "{},{},{},{},{},{},{}".format(repo_name, slope, intercept, r2, len(x), cumsum[-1], dydx)


def index_mapper(line):
    repo_name, slope, intercept, r2, entries, integral, derivative = line

    index = (0.4 * float(integral) + 0.6 * float(derivative))
    index *= float(entries) / 52.0
    if integral:
        index += float(derivative) / float(integral)

    linked = "=HYPERLINK(\"https://github.com/{}\"; \"{}\")".format(repo_name, repo_name) # This is for google sheets

    return (linked, index)

ts = hive_context.table("srn334.{}".format(INPUT_TABLE))
ts.registerTempTable('{}'.format(INPUT_TABLE))

ts = hive_context.sql("SELECT * FROM {} WHERE week < {}".format(INPUT_TABLE, str(TRAINING_CUT_OFF)))

# We place the time series in our DF so we can pass it through to the mapper
df = ts.groupBy("repo").agg(f.collect_list("week"), f.collect_list("cumsum"))

# We do the mapping, each map task is a linear regression task.
result = df.rdd.map(tuple).map(map_linear_regression_derivative)

# Now we use those results to calculate the index
result = result.map(index_mapper)

# Finally we take the steps to output our index in descending order
result = result.toDF(['repo', 'index']).orderBy('index', ascending=False)

result = result.rdd.map(tuple).map(lambda (repo_name, index): "{},{}".format(repo_name, index))

result.saveAsTextFile(OUTPUT_DIR)
