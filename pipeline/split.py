"""
    In order to evaluate the performance of our analytic, we need to split our data in to training and
    evaluation sets. There are two ways we could do this: (1) split into two sets of repositories, we
    train on one set of repositories and evaluate on the other set, or (2) split by week number, we
    train on a predicate subset of the weeks and evaluate on the subsequent weeks.

    We choose the latter approach because the former does not jibe with our strategy for training the
    model – the model is trained with the particular repository in mind.
"""
from pyspark.sql import functions as f
from pipeline.config import TRAIN_WEEKS, TARGET


def get_train_split(df):
    return df.filter(f.Column('WEEK').between(0, TRAIN_WEEKS)).withColumn(TARGET, f.Column(TARGET))


def get_eval_split(df):
    return df.filter(f.Column('WEEK') > TRAIN_WEEKS).withColumn(TARGET, f.Column(TARGET))
