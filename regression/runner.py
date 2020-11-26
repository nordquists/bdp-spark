"""

    The purpose of the runner is to parallelize the running of the linear regression models.

    Broadly:

    MAP(each repo name -> linearRegression(repo_name))

"""
from regression.linear import LinearRegression
import numpy as np






def map_linear_regression(hive_context, repo_name):
    # We have to
    ts = hive_context.table("srn334.ts_day")
    ts.registerTempTable('ts_day')

    ts = hive_context.sql("SELECT * FROM ts_day where repo = '{}'".format(repo))

    lr = LinearRegression()

    x = np.array(train.select('day').collect()).flatten()
    y = np.array(train.select('score').collect()).flatten()

    predictions = lr.predict(x)
    rmse, r2 = lr.evaluate(predictions, y)


