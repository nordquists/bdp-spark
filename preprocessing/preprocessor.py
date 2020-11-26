import datetime
import pyspark.sql.functions as f


def index_map(line):
    repo = line[0]
    type = line[1]
    week = float(line[2])
    count = int(line[3])

    if type == 'ForkEvent':
        count = 1.3 * count
    elif type == 'WatchEvent':
        count = 1 * count
    elif type == 'PushEvent':
        count = 0.9 * count

    return "{},{}".format(repo, week), count


def adjust_granularity(rdd, granularity='month'):
    if granularity == 'month':
        result = rdd.map(lambda row: ("{},{},{}".format(row[0], row[1], datetime.datetime.strptime(row[3],
                                        '%Y-%m-%d %H:%M:%S %Z').month),row[4]))\
                                        .reduceByKey(lambda a, b: a + b)

    result = result.map(lambda (x, y): (x.split(',')[0], x.split(',')[1], x.split(',')[2], y))
    result = result.map(lambda (x, y, z, a): "{},{},{},{}".format(x, y, str(z), str(a)))

    return result


def create_index(rdd, weight_fork=1.3, weight_watch=1, weight_push=0.9):
    result = rdd.map(lambda line: line.split(","))
    result = result.map(index_map).reduceByKey(lambda a, b: a + b)
    result = result.map(lambda (x, y): (x.split(',')[0], x.split(',')[1], y))
    result = result.map(lambda (x, y, z): "{},{},{}".format(x, str(y), str(z)))

    return result


def apply_filter(rdd, granularity='month', min_score=1000):
    original_df = rdd.map(lambda line: line.split(",")).toDF(["repo", granularity, "score"])
    sum_df = rdd.map(lambda line: line.split(",")).map(lambda (x, y, z): (x, int(z))).reduceByKey(
        lambda a, b: a + b).toDF(["repo", "score"])

    sum_df = sum_df.filter(f.col('score') > min_score)
    result_df = original_df.join(sum_df, ["repo"], "left_semi")

    return result_df.rdd.map(tuple).map(lambda (repo, week, score): "{},{},{}".format(repo,week,str(score)))