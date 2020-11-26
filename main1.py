from regression.linear import LinearRegression
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pipeline.split import get_train_split, get_eval_split
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

train = get_train_split(ts)
eval = get_eval_split(ts)

x = np.array(train.select('week').collect()).flatten()
y = np.array(train.select('score').collect()).flatten()

lr = LinearRegression()

lr.fit(x, y)

x_hat = np.array(eval.select('week').collect()).flatten()
y_hat = np.array(eval.select('score').collect()).flatten()

predictions = lr.predict(x_hat)

print(lr.get_slope())

rmse, r2 = lr.evaluate(predictions, y_hat)

plt.scatter(x, y)
plt.show()