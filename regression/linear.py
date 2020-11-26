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
        self.__slope = self.__fitted[0]
        self.__intercept = self.__fitted[1]

    def predict(self, x):
        if self.__fitted is None:
            return None

        predictions = numpy.polyval(self.__fitted, x)

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

    def get_intercept(self):
        return self.__intercept
