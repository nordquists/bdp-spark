import gc
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import *
from pipeline.config import TRAIN_WEEKS
from pipeline.features import apply_pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import *
from pipeline.split import get_train_split, get_eval_split

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

# Register our time series data
ts = hive_context.table("srn334.ts_weekly")
ts.registerTempTable('ts_weekly')

ts = hive_context.sql("SELECT * FROM ts_weekly WHERE week > 40")

temp = apply_pipeline(ts)

# ts.unpersist()
# del ts
# gc.collect()





# temp.createOrReplaceTempView("temp_table")
# hive_context.sql("create table feature_table as select * from temp_table")

# result = temp.rdd\
#     .map(tuple).map(lambda (repo, week, score, repo_indexed, repo_indexed_encoded, features): "{},{},{}".format(repo,str(features),str(score)))\
#     .saveAsTextFile("hdfs://dumbo/user/srn334/final/test_output1/")


gbt = GBTRegressor(featuresCol='features', labelCol='score')
train = get_train_split(temp)
eval = get_eval_split(temp)
fitted = gbt.fit(train)
y = (fitted.transform(eval).withColumn("prediction").withColumn("score"))
eval_ = RegressionEvaluator(labelCol="score", predictionCol="prediction", metricName="rmse")
rmse = eval_.evaluate(y)
print('rmse is %.2f' %rmse)
mae = eval_.evaluate(y, {eval_.metricName: "mae"})
print('mae is %.2f' %mae)
r2 = eval_.evaluate(y, {eval_.metricName: "r2"})
print('r2 is %.2f' %r2)