"""
    This file is called by the graphing file to run the algorithms we ask to graph. Nothing new is done here.
"""
from linear_regression import LinearRegression
from sma import movingaverage
from arima import arima
import numpy as np

# Constants
ALGORITHMS = {
    "SMA3": 1,
    "ARIMA": 0,
    "LR": 2
}

ALGORITHMS_REVERSED = {
    1: "SMA3",
    0: "ARIMA",
    2: "LR"
}


def run_algorithms(algorithms, x, y, x_hat):
    results = []
    for algorithm in algorithms:
        if algorithm == 0:
            fit = arima(y)
            pred = fit.predict(1, x_hat[-1] if x_hat else x[-1], typ='levels')
            results.append([algorithm, (range(0, x_hat[-1] if x_hat else x[-1]), pred)])
        elif algorithm == 1:
            y_1 = movingaverage(y, 3)
            results.append([algorithm, (x, y_1)])
        elif algorithm == 2:
            lr = LinearRegression()
            lr.fit(x, y)
            y_1 = lr.predict(x)
            y_hat = lr.predict(x_hat)
            rmse, r2 = lr.evaluate(np.append(y_1, y_hat, 0), y)
            print(r2)
            results.append([algorithm, (np.append(x, x_hat, 0), np.append(y_1, y_hat, 0))])

    return results
