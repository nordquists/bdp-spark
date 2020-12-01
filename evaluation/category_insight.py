"""
    This is written in python 3.

    To run this file, use (Spark is not used for this file because it is just comparing CSVs):
        $   python category_insight.py

    This file serves as a way to extend our analytic by comparing the top 100 repositories in our analytic and the
    evaluation lists.

    It serves only as a simple extension so we can get more insight into our analytic.
"""
from collections import defaultdict
import sys

NAMES = [
    "analytic",
    "eval2",
    "eval1"
]

FILES = [
    "./references/analytic.list",
    "./references/eval2.list",
    "./references/eval1.list"
]

category_map = {}


def load_file(input_file):
    repos = []
    with open(input_file, 'r') as input:
        for line in input:
            repo_name, category = line.split(',')
            category = category.strip()
            repos.append(repo_name)

            if repo_name not in category_map:
                category_map[repo_name] = category

    return repos


def load_categories(files):
    output = []
    for file in files:
        output.append(load_file(file))

    return output


def calc_percentage(list):
    categories = defaultdict(float)
    total = 0.0
    for repo in list:
        if repo not in category_map:
            print(f"{repo} not in map")
            exit()

        category = category_map[repo]
        categories[category] += 1.0
        total += 1.0

    for category in categories.keys():
        categories[category] /= total

    return categories


def output_percentages(name, percentages):
    results = list(zip(percentages.keys(), percentages.values()))
    results.sort(key=lambda x:x[1], reverse=True)

    with open(f"./percentages/{name}.percentages", 'w') as output:
        for category, percentage in results:
            output.write(f"{category},{percentage:.2f}\n")

        output.write(str(sum(percentages.values())))


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: python category_insight.py")
        sys.exit(1)

    analytic, eval1, eval2 = load_categories(FILES)

    analytic_percentages = calc_percentage(analytic)
    eval1_percentages = calc_percentage(eval1)
    eval2_percentages = calc_percentage(eval2)
    
    output_percentages(NAMES[0], analytic_percentages)
    output_percentages(NAMES[1], eval1_percentages)
    output_percentages(NAMES[2], eval2_percentages)



