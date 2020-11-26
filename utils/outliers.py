from pyspark.sql import *
import pyspark.sql.functions as f
import numpy as np


def exclude_outliers(y, df):
    quartile1, quartile2 = np.percentile(y, 10), np.percentile(y, 90)
    filtered_df = df.filter(f.col('score').between(quartile1, quartile2))
    return filtered_df
