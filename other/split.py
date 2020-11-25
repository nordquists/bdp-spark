from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.functions import *

DEV_SPLIT = 0.8
EVAL_SPLIT = 1 - DEV_SPLIT

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)


# input = sc.textFile("hdfs://dumbo/user/srn334/final/indexed_data")

ts = hive_context.table("srn334.ts_weekly")

ts.registerTempTable('ts_weekly')

df = hive_context.sql("SELECT DISTINCT repo FROM ts_weekly")
df_index = df.select("*").withColumn("id", monotonically_increasing_id())

length = df.count()

dev_length = int(length * DEV_SPLIT)

dev_df = df_index.where(col('id') <= dev_length)
eval_df = df_index.where(col('id') > dev_length)

# dev_df.write.format('com.databricks.spark.csv').save('hdfs://dumbo/user/srn334/final/dev_set/')

dev_df.toPandas().to_csv('hdfs://dumbo/user/srn334/final/dev_set/')
eval_df.toPandas().to_csv('eval_set.csv')

dev_df.createOrReplaceTempView("temp_dev")
eval_df.createOrReplaceTempView("temp_eval")



hive_context.sql("CREATE TABLE dev_set AS SELECT * FROM temp_dev")
hive_context.sql("CREATE TABLE eval_set AS SELECT * FROM temp_eval")

