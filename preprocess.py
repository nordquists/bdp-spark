from pyspark import SparkContext
from pyspark.sql import HiveContext
import datetime
import pyspark.sql.functions as f
import math

# from preprocessing.preprocessor import adjust_granularity, create_index, apply_filter

def index_map(line):
    repo = line[0]
    type = line[1]
    week = float(line[2])
    count = int(line[3])

    # if type == 'ForkEvent':
    #     count = count
    # elif type == 'WatchEvent':
    #     count = count
    # elif type == 'PushEvent':
    #     count = count

    return "{},{}".format(repo, week), count


def adjust_granularity(rdd, granularity='month'):
    if granularity == 'month':
        result = rdd.map(lambda row: ("{},{},{}".format(row[0], row[1], datetime.datetime.strptime(row[3],'%Y-%m-%d %H:%M:%S %Z').month),row[4])).reduceByKey(lambda a, b: a + b)
    elif granularity == 'week':
        result = rdd.map(lambda row: ("{},{},{}".format(row[0], row[1], str(datetime.datetime.strptime(row[3],'%Y-%m-%d %H:%M:%S %Z').isocalendar()[1])),row[4])).reduceByKey(lambda a, b: a + b)

    result = result.map(lambda (x, y): (x.split(',')[0], x.split(',')[1], x.split(',')[2], y))
    result = result.map(lambda (x, y, z, a): "{},{},{},{}".format(x, y, str(z), str(a)))

    return result


def create_index(rdd, weight_fork=1.3, weight_watch=1, weight_push=0.9):
    result = rdd.map(lambda line: line.split(","))
    result = result.map(index_map).reduceByKey(lambda a, b: a + b)
    result = result.map(lambda (x, y): (x.split(',')[0], x.split(',')[1], y))
    result = result.map(lambda (x, y, z): "{},{},{}".format(x, str(y), str(z)))

    return result


def apply_filter(rdd, granularity='month', min_score=500):
    original_df = rdd.map(lambda line: line.split(",")).toDF(["repo", granularity, "score"])
    sum_df = rdd.map(lambda line: line.split(",")).map(lambda (x, y, z): (x, float(z))).reduceByKey(
        lambda a, b: a + b).toDF(["repo", "score"])

    sum_df = sum_df.filter(f.col('score') > min_score)
    result_df = original_df.join(sum_df, ["repo"], "left_semi")

    return result_df.rdd.map(tuple).map(lambda (repo, week, score): "{},{},{}".format(repo,week,str(score)))


INPUT_DIR = "hdfs://dumbo/user/srn334/final/output/part-r-00000"
OUTPUT_DIR = "hdfs://dumbo/user/srn334/final/preprocessed_weekly/"

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

sc.setLogLevel("WARN")

rdd = sc.textFile(INPUT_DIR)
rdd = rdd.map(lambda line: line.split(","))

rdd = adjust_granularity(rdd, granularity='week')

# rdd = create_index(rdd, weight_fork=0.5, weight_watch=2, weight_push=0.1)

# rdd = apply_filter(rdd, granularity='week', min_score=300)

rdd.saveAsTextFile(OUTPUT_DIR)
