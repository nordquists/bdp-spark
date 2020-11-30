"""
    To run this file, use:
        $   spark-submit --num-executors 10 --driver-memory 30G --executor-memory 12G graph.py

    This file is used to make various graphs. To make the graphs, I just adjust the variables to get the data I want.
    So there is no definite way of running this. It is not vital to the final analytic though, it just is here to
    make the graphs for the presentation and paper.
"""
from algorithms import run_algorithms, ALGORITHMS, ALGORITHMS_REVERSED
from split import get_eval_split, get_train_split
from outliers import exclude_outliers
from pyspark import SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import matplotlib

matplotlib.use('tkagg')

sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
sc.setLogLevel("OFF")

# From http://www.randalolson.com/2014/06/28/how-to-make-beautiful-data-visualizations-in-python-with-matplotlib/
TABLEAU_20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

for i in range(len(TABLEAU_20)):
    r, b, g = TABLEAU_20[i]
    TABLEAU_20[i] = (float(r)/256.0, float(b)/256.0, float(g)/256.0)

# -----------------------------------------------------------------

# Adjustables
REPO_NAME = "CSSEGISandData/COVID-19"
TABLE_NAME = "weekly_cumulative"
TYPE = "score"
TO_PLOT = [
    # ALGORITHMS["SMA3"],
    # ALGORITHMS["ARIMA"],
    ALGORITHMS["LR"]
]

plt.figure(figsize=(16, 12))

# -----------------------------------------------------------------

ts = hive_context.table("srn334.{}".format(TABLE_NAME))
ts.registerTempTable('{}'.format(TABLE_NAME))

ts = hive_context.sql("SELECT * FROM {} where repo = '{}'".format(TABLE_NAME, REPO_NAME))

ts = ts.orderBy('week')

# ts = exclude_outliers(np.array(ts.select('cumsum').collect()).flatten(), ts)

print(ts.show(30))

train = get_train_split(ts, TYPE)
eval = get_eval_split(ts, TYPE)

x = np.array(train.select('week').collect()).flatten()
y = np.array(train.select(TYPE).collect()).flatten()

x_hat = np.array(eval.select('week').collect()).flatten()
y_hat = np.array(eval.select(TYPE).collect()).flatten()

assert len(x) == len(y)

results = run_algorithms(TO_PLOT, x, y, x_hat)

for result in results:
    algorithm_id = result[0]
    algorithm_color = TABLEAU_20[algorithm_id]
    algorithm_name = ALGORITHMS_REVERSED[algorithm_id]

    x_plot, y_plot = result[1]

    print(x_plot, y_plot)

    plt.plot(list(x_plot), list(y_plot), label=algorithm_name, color=algorithm_color)

plt.bar(x, y, color=TABLEAU_20[5])
# plt.scatter(x, y, color=TABLEAU_20[4])
plt.scatter(x_hat, y_hat, color=TABLEAU_20[8])

# -----------------------------------------------------------------

# Customizing plot, following design from from http://www.randalolson.com/2014/06/28/how-to-make-beautiful-data-visualizations-in-python-with-matplotlib/
# Designing figures
ax = plt.subplot(111)
ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)

ax.get_xaxis().tick_bottom()
ax.get_yaxis().tick_left()

if len(y_hat) != 0:
    y_max = int(max(max(y), max(y_hat)))
    y_min = int(min(min(y), min(y_hat)))
    increment_size = int(y_max // 10)
else:
    y_max = int(max(y))
    y_min = int(min(y))
    increment_size = int(y_max // 10)

print(y_min, y_max)

# plt.yticks(range(y_min - 2000, y_max+2000, increment_size), [str(x) for x in range(y_min - 2000, y_max+2000, increment_size)], fontsize=14)
plt.xticks(fontsize=20)

for y in range(increment_size, y_max, increment_size):
    plt.plot(range(0, 45), [y] * len(range(0, 45)), "--", lw=0.5, color="black", alpha=0.3)
#
plt.tick_params(axis="both", which="both", bottom="off", top="off",
                labelbottom="on", left="off", right="off", labelleft="on")

# ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2e'))
# ax.set_yscale('log')

plt.ylim(y_min//2, y_max*1.1)


plt.title("{} Activity of {} over time (2020)".format('Cumulative' if TYPE== 'cumsum' else '',REPO_NAME), fontsize=30)
plt.xlabel('Weeks in 2020', fontsize=25)
plt.ylabel('Activity Index', fontsize=25)

# plt.text("Note: Models tested were only trained on the first 40 weeks (points in blue).", fontsize=10)
plt.legend()

plt.show(dpi=1200)

plt.savefig("graphs/figure1.png", bbox_inches="tight", dpi=1200)
