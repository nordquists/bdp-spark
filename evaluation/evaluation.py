"""


    To run this file, use (Spark is not used for this file because it is just comparing CSVs):
        $   python evaluation.py <reference_file> <candidate_file>

    This file serves as a tool to evaluate our analytic on one of the variety of evaluation metrics that I have
    decided on.

    The evaluation metric we are looking at is how well our system matches current ranking systems for top GitHub
    repositories. We do this using a standard accuracy, precision, recall, and F-score system. These quantities are
    as they usually with respect to false positives/negatives and true positives/negatives and the precise formulae
    can be found in the code below.

    Note: for the purpose of this form of evaluation, we consider the system a binary classifier into two classes –
          in the top 100 or not in the top 100. Rankings are not considered.
"""
import sys


def load_file(input_file):
    repos = []
    with open(input_file, 'r') as input:
        for line in input:
            repo_name = line.split(',')
            repos.append(repo_name)

    return repos


def write_scores(name, accuracy, precision, recall, f):
    with open(f"./results/{name}.results", 'w') as output:
        output.write(f"ACCURACY:\t{accuracy}\n")
        output.write(f"PRECISION:\t{precision}\n")
        output.write(f"RECALL:\t{recall}\n")
        output.write(f"F-SCORE:\t{f}\n")


def evaluate(reference_file, candidate_file):
    references = load_file(reference_file)
    candidates = load_file(candidate_file)

    references_set = set(references)
    candidates_set = set(candidates)

    length = len(references)

    intersection = float(len(references_set.intersection(candidates_set)))

    # Notice these calculations will be the same because our length is bounded and always 100.
    accuracy = intersection / length
    precision = intersection / length
    recall = intersection / length
    f = (2 * recall * precision) / (recall + precision) if (recall + precision) else 0

    return accuracy, precision, recall, f


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python evaluate.py <reference_file> <candidate_file>")
        sys.exit(1)

    accuracy, precision, recall, f = evaluate(sys.argv[1], sys.argv[2])

    write_scores(sys.argv[1], accuracy, precision, recall, f)

