"""
    This file is called by the graphing file to run the algorithms we ask to graph. Nothing new is done here.
"""
from linear_regression import LinearRegression
from sma import movingaverage
from arima import arima

# Constants
ALGORITHMS = {
    "SMA3": 0,
    "ARIMA": 1,
    "LR": 2
}

ALGORITHMS_REVERSED = {
    0: "SMA3",
    1: "ARIMA",
    3: "LR"
}


def run_algorithms(algorithms, x, y, x_hat):
    results = []
    for algorithm in algorithms:
        if algorithm == 0:
            fit = arima(y)
            print(x_hat[-1])
            pred = fit.predict(0, x_hat[-1], typ='levels')
            y_1, y_hat = pred[:x[-1]], pred[x_hat[0]:]
            results.append([algorithm, (x + x_hat, y_1 + y_hat)])
        elif algorithm == 1:
            y_1 = movingaverage(y, 3)
            results.append([algorithm, (x, y_1)])
        elif algorithm == 2:
            lr = LinearRegression()
            lr.fit(x, y)
            y_1 = lr.predict(x_hat)
            y_hat = lr.predict(x_hat)
            results.append([algorithm, (x + x_hat, y_1 + y_hat)])

    return results
