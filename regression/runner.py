"""

    The purpose of the runner is to parallelize the running of the linear regression models.

    Broadly:

    MAP(each repo name -> linearRegression(repo_name))

"""
from regression.linear import LinearRegression
import numpy as np






def map_linear_regression(hive_context, table_name, repo_name):
    # We have to
    ts = hive_context.table("srn334.{}".format(table_name))
    ts.registerTempTable('{}'.format(table_name))

    ts = hive_context.sql("SELECT * FROM {} where repo = '{}'".format(table_name, repo_name))

    lr = LinearRegression()

    x = np.array(ts.select('day').collect()).flatten()
    y = np.array(ts.select('score').collect()).flatten()

    predictions = lr.predict(x)
    rmse, r2 = lr.evaluate(predictions, y)


