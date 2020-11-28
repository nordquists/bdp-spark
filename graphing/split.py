import pyspark.sql.functions as f


def get_train_split(df):
    return df.filter(f.col('week').between(0, 40)).withColumn("score", f.col("score"))


def get_eval_split(df):
    return df.filter(f.col('week') > 40).withColumn("score", f.col("score"))
