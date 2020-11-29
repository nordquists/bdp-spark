import pyspark.sql.functions as f


def get_train_split(df):
    return df.filter(f.col('week').between(20, 50)).withColumn("cumsum", f.col("cumsum"))


def get_eval_split(df):
    return df.filter(f.col('week') > 50).withColumn("cumsum", f.col("cumsum"))
