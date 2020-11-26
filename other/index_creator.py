"""

Index combines the values of WATCH, FORK, and PUSH events with the following equation:

    num_watch^2 + fork^3 + push
"""
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

input = sc.textFile("hdfs://dumbo/user/srn334/final/indexed_data_days/")

newdata = input.map(lambda line: line.split(","))


def mapFunc(lines):
    repo = lines[0].replace("'","").replace('(', "").strip()
    type = lines[1]
    week = lines[2]
    count = int(lines[4].replace("'", "").replace('u',"").replace(')', "").strip())

    if type == 'ForkEvent':
        count = 3 * 0
    elif type == 'WatchEvent':
        count = 1 * count
    elif type == 'PushEvent':
        count = 1 * 0

    return "{},{},".format(repo, week), count


result = newdata.map(mapFunc).reduceByKey(lambda a, b: a + b)
result_split = result.map(lambda (x, y): (x.split(',')[0], x.split(',')[1], y))
result2 = result_split.map(lambda (x, y, z): "{},{},{}".format(x, str(y), str(z)))

result2.saveAsTextFile("hdfs://dumbo/user/srn334/final/indices_day/")
