from pyspark.sql import *
import pyspark.sql.functions as f


def exclude_outliers(df):
    quantiles = df.approxQuantile('score', [0.25, 0.75], 0.5)
    filtered_df = df.filter(f.col('score').between(quantiles[0], quantiles[1]))
    return filtered_df
