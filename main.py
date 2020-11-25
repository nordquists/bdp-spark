import gc
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import *
from pipeline.config import TRAIN_WEEKS, schema_ts
from pipeline.features import apply_pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import *
from pipeline.split import get_train_split, get_eval_split
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.feature import PCA


sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

# Register our time series data
# ts = hive_context.table("srn334.ts_weekly")
# ts.registerTempTable('ts_weekly')
#
# ts = hive_context.sql("SELECT * FROM ts_weekly WHERE week > 40")

input = sc.textFile("hdfs://dumbo/user/srn334/final/indices")

newdata = input.map(lambda line: line.split(","))

ts = hive_context.createDataFrame(newdata, schema=schema_ts)

# temp = apply_pipeline(ts)
category = "repo"

ts = ts.fillna({ 'score': 0, 'week': 0, 'repo': '' })

indexer = StringIndexer(inputCol=category,
                         outputCol="{}_indexed".format(category), handleInvalid='skip')

one_hot_encoder = OneHotEncoder(dropLast=True, inputCol=indexer.getOutputCol(),
                                 outputCol="{}_encoded".format(indexer.getOutputCol()))

# This steps puts our features in a form that will be understood by the regression models
features = VectorAssembler(inputCols=[one_hot_encoder.getOutputCol()] + ['week'],
                            outputCol="features")

pca = PCA(k=5, inputCol="features", outputCol="pcaFeatures")

pipeline = Pipeline(stages=[indexer, one_hot_encoder, features, pca])

model = pipeline.fit(ts)
temp = model.transform(ts)


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