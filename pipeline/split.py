import pyspark.sql.functions as f
from pipeline.config import TRAIN_WEEKS, TARGET


def get_train_split(df):
    return df.filter(f.col('day').between(0, TRAIN_WEEKS)).withColumn(TARGET, f.col(TARGET))


def get_eval_split(df):
    return df.filter(f.col('day') > TRAIN_WEEKS).withColumn(TARGET, f.col(TARGET))
