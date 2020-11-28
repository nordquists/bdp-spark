"""
    To run this file, use:
        $   spark-submit --num-executors 10 --driver-memory 30G --executor-memory 12G graph.py

    This file is used to make various graphs. To make the graphs, I just adjust the variables to get the data I want.
    So there is no definite way of running this. It is not vital to the final analytic though, it just is here to
    make the graphs for the presentation and paper.
"""
from algorithms import run_algorithms, ALGORITHMS, ALGORITHMS_REVERSED
from split import get_eval_split, get_train_split
from pyspark import SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
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

# -----------------------------------------------------------------

# Adjustables
REPO_NAME = "facebook/react-native"
TABLE_NAME = "ts_weekly_cumulative_preprocessed"
TO_PLOT = [
    ALGORITHMS["SMA3"],
    ALGORITHMS["ARIMA"],
    ALGORITHMS["LR"]
]

# -----------------------------------------------------------------

ts = hive_context.table("srn334.{}".format(TABLE_NAME))
ts.registerTempTable('{}'.format(TABLE_NAME))

ts = hive_context.sql("SELECT * FROM {} where lower(repo) = '{}'".format(TABLE_NAME, REPO_NAME))

train = get_train_split(ts)
eval = get_eval_split(ts)

x = np.array(train.select('week').collect()).flatten()
y = np.array(train.select('score').collect()).flatten()

x_hat = np.array(eval.select('week').collect()).flatten()
y_hat = np.array(eval.select('score').collect()).flatten()

assert len(x) == len(y)

results = run_algorithms(TO_PLOT, x, y, x_hat)

for result in results:
    algorithm_id = result[0]
    algorithm_color = TABLEAU_20[algorithm_id]
    algorithm_name = ALGORITHMS_REVERSED[algorithm_id]

    x_plot, y_plot = result[1]

    plt.plot(x_plot, y_plot, label=algorithm_name, color=algorithm_color)

plt.scatter(x, y, color=TABLEAU_20[-5])
plt.scatter(x_hat, y_hat, color=TABLEAU_20[-1])

# -----------------------------------------------------------------

# Customizing plot, following design from from http://www.randalolson.com/2014/06/28/how-to-make-beautiful-data-visualizations-in-python-with-matplotlib/
plt.figure(figsize=(12, 9))

# Designing figures
ax = plt.subplot(111)
ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)

ax.get_xaxis().tick_bottom()
ax.get_yaxis().tick_left()

plt.yticks(range(0, 91, 10), [str(x) for x in range(0, 91, 10)], fontsize=14)
plt.xticks(fontsize=14)

for y in range(10, 91, 10):
    plt.plot(range(0, 52), [y] * len(range(0, 52)), "--", lw=0.5, color="black", alpha=0.3)

plt.tick_params(axis="both", which="both", bottom="off", top="off",
                labelbottom="on", left="off", right="off", labelleft="on")

plt.text(1995, 93, "Activity of {} over time (2020)", fontsize=17, ha="center")
plt.xlabel('Weeks in 2020')
plt.ylabel('Activity Index')

plt.text(1966, -8, "Note: Models tested were only trained on the first 40 weeks (points in blue).", fontsize=10)
plt.legend()

plt.show()

plt.savefig("figure1.png", bbox_inches="tight")
