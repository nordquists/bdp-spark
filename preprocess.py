from pyspark import SparkContext
from pyspark.sql import HiveContext
from preprocessing.preprocessor import Preprocessor

INPUT_DIR = "hdfs://dumbo/user/srn334/final/output/part-r-00000"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/preprocessed/"

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

sc.setLogLevel("WARN")

rdd = sc.textFile(INPUT_DIR)
rdd = rdd.map(lambda line: line.split(","))

rdd = Preprocessor.adjust_granularity(rdd, granularity='month')

rdd = Preprocessor.create_index(rdd, weight_fork=1.3, weight_watch=1, weight_push=0.9)

rdd = Preprocessor.apply_filter(rdd, granularity='month', min_score=1000)

rdd.saveAsTextFile(OUTPUT_DIR)
