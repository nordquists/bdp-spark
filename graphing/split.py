import pyspark.sql.functions as f


def get_train_split(df):
    return df.filter(f.col('week').between(0, 35)).withColumn("cumsum", f.col("cumsum"))


def get_eval_split(df):
    return df.filter(f.col('week') > 35).withColumn("cumsum", f.col("cumsum"))
