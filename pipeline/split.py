from pyspark.sql import functions as f
from pipeline.config import TRAIN_WEEKS, TARGET


def get_train_split(df):
    return df.filter(f.Column('WEEK').between(0, TRAIN_WEEKS)).withColumn(TARGET, f.Column(TARGET))


def get_eval_split(df):
    return df.filter(f.Column('WEEK') > TRAIN_WEEKS).withColumn(TARGET, f.Column(TARGET))
