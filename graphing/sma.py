"""
    Nothing is new in this file either, just have to have a copy here because the file tree in Python is hard to deal
    with.
"""
import numpy as np


def movingaverage(interval, window_size):
    window= np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window, 'same')
