import pyspark.sql.functions as f


def get_train_split(df, type):
    return df.filter(f.col('week').between(0, 50)).withColumn(type, f.col(type))


def get_eval_split(df, type):
    return df.filter(f.col('week') > 50).withColumn(type, f.col(type))
