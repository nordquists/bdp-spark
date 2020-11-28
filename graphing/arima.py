"""
    Nothing is new in this file either, just have to have a copy here because the file tree in Python is hard to deal
    with.
"""
from statsmodels.tsa.arima_model import ARIMA


def arima(y):
    order = (5, 1, 1)
    model = ARIMA(y, order, freq='W')
    fit = model.fit(transparams=True)
    return fit
