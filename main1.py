from regression.linear import LinearRegression
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pipeline.split import get_train_split, get_eval_split
from utils.outliers import exclude_outliers
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use('tkagg')

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
sc.setLogLevel("WARN")

# Register our time series data
ts = hive_context.table("srn334.ts_filtered")
ts.registerTempTable('ts_filtered')

ts = hive_context.sql("SELECT * FROM ts_filtered where lower(repo) = 'facebook/react-native'")

ts = ts.fillna({'score': 0, 'week': 0, 'repo': ''})

# ts = exclude_outliers(np.array(ts.select('score').collect()).flatten(), ts)

train = get_train_split(ts)
eval = get_eval_split(ts)

x = np.array(train.select('week').collect()).flatten()
y = np.array(train.select('score').collect()).flatten()

lr = LinearRegression()

lr.fit(x, y)

x_hat = np.array(eval.select('week').collect()).flatten()
y_hat = np.array(eval.select('score').collect()).flatten()

predictions = lr.predict(x_hat)

print("slope {}".format(str(lr.get_slope())))

rmse, r2 = lr.evaluate(predictions, y_hat)

print("rmse {}, r2 {}".format(str(rmse), str(r2)))

x_plot = range(0, 52)
y_plot = lr.predict(x_plot)

plt.scatter(x, y, color="blue")
plt.scatter(x_hat, y_hat, color="red")
plt.plot(x_plot, y_plot, color="red")
plt.show()