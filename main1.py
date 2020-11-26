from regression.linear import LinearRegression
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pipeline.split import get_train_split, get_eval_split
from utils.outliers import exclude_outliers
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from statsmodels.tsa.arima_model import ARIMA

matplotlib.use('tkagg')

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
sc.setLogLevel("WARN")

# Register our time series data
ts = hive_context.table("srn334.ts_day")
ts.registerTempTable('ts_day')

ts = hive_context.sql("SELECT * FROM ts_day where lower(repo) = 'jepsen-io/jepsen'") # facebook/react-native

ts = ts.fillna({'score': 0, 'day': 0, 'repo': ''})

repo_name = ts.select('repo').collect()[0][0]
days = ts.select('day').collect()
missing_x = set(range(1, 259))

for day in days:
    if day[0] in missing_x:
        missing_x.remove(day[0])

for day in missing_x:
    new_row = hive_context.createDataFrame([(repo_name, day, 0)], ['repo', 'day', 'score'])
    ts = ts.unionAll(new_row)

# ts = exclude_outliers(np.array(ts.select('score').collect()).flatten(), ts)

train = get_train_split(ts)
eval = get_eval_split(ts)

x = np.array(train.select('day').collect()).flatten()
y = np.array(train.select('score').collect()).flatten()
#
lr = LinearRegression()

lr.fit(x, y)

# model = sm.tsa.statespace.SARIMAX(y, trend='c', order=(1,1,1))
# fit = SARIMAX(y,order=(7,1,7),freq='W',seasonal_order=(0,0,0,0),
#                                  enforce_stationarity=False, enforce_invertibility=False,).fit()
order = (5, 1, 1)
model = ARIMA(y, order, freq='D')
fit = model.fit(transparams=True)

# for p in range(6):
#     for d in range(2):
#         for q in range(4):
#             try:
#                 fit=ARIMA(y,(p,d,q), freq="D").fit(transparams=True)
#
#                 x1= p,d,q
#                 print (x1)
#             except:
#                 pass


x_hat = np.array(eval.select('day').collect()).flatten()
y_hat = np.array(eval.select('score').collect()).flatten()


# from sklearn.metrics import mean_squared_error
# pred = fit.predict(40,52, typ='levels')
# print('ARIMA model MSE:{}'.format(mean_squared_error(y_hat,pred)))


predictions = lr.predict(x_hat)

print("slope {}".format(str(lr.get_slope())))

rmse, r2 = lr.evaluate(predictions, y_hat)

print("rmse {}, r2 {}".format(str(rmse), str(r2)))


def movingaverage(interval, window_size):
    window= np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window, 'same')

y_av = movingaverage(y, 30)

x_plot = range(0, 365)
y_plot = fit.predict(1, 365, typ='levels')
y_plot_lr = lr.predict(x_plot)

plt.scatter(x, y, color="blue")
plt.scatter(x_hat, y_hat, color="red")
plt.plot(x_plot, y_plot, label="ARIMA",color="red")
plt.plot(x_plot, y_plot_lr,label="LR", color="green")
plt.plot(range(1, 260), y_av, label="30d SMA", color="purple")

# plt.xlim([0, 325])
# plt.ylim([50, 150])

plt.title('GitHub Repo Activity Over Time')
plt.xlabel('Time (Days)')
plt.ylabel('Activity Index')

plt.legend()

plt.show()