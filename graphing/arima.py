"""
    Nothing is new in this file either, just have to have a copy here because the file tree in Python is hard to deal
    with.
"""
from statsmodels.tsa.arima_model import ARIMA


def arima(y):
    for p in range(6):
        for d in range(2):
            for q in range(4):
                try:
                    fit=ARIMA(y,(p,d,q), freq="W").fit(transparams=True)

                    x1= p,d,q
                except:
                    pass

    return fit
