import numpy


class LinearRegression:
    def __init__(self):
        self.__slope = None
        self.__intercept = None
        self.__r_squared = None
        self.__p_value = None
        self.__std_err = None
        self.__fitted = None

    def fit(self, x, y, order=1):
        self.__fitted = numpy.polyfit(x, y, order)

    def predict(self, x):
        if not self.__fitted:
            return None

        predictions = numpy.polyval(self.__fitted, x)
        self.__intercept = predictions[0]
        self.__slope = predictions[1]

        return predictions

    def evaluate(self, predictions, y):
        abs_error = predictions - y
        squared_error = numpy.square(abs_error)
        mean_squared_error = numpy.mean(squared_error)
        rmse = numpy.sqrt(mean_squared_error)
        r2 = 1.0 - (numpy.var(abs_error) / numpy.var(y))

        return rmse, r2

    def get_slope(self):
        return self.__slope