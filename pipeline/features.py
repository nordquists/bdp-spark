# coding: utf-8
"""
    We can be pretty confident we have a correlation between our independent variable (activity index)
    and our dependent variable. How time plays a role in the activity index is not a definite function
    for all repositories.

    Our goal in this file is to extract and transform features into a dataframe that can be used by the
    SparkML library, broadly this looks like:

        Features        Score
        [1,4.2,4,1]     19

    The feature schema we are looking for is extremely simple – we have three columns (1) repo id, (2) week number, and
    (3) activity index.

    The idea here is the amount of data for any single repository is relatively limited, so we train on all repositories
    and use that particular repository's id as a feature in the regression. An idea similar to this one was successfully
    implemented in the kaggle competition: https://www.kaggle.com/paulorzp/log-ma-and-days-of-week-means-lb-0-529.
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler


category = "repo"


def apply_pipeline(df):
    # We are defining rules for our pipeline to standardize our data for training and for eventually running the model.
    # imputer = [Imputer(inputCols=df.columns,
    #                    outputCols=["{}_imputed".format(col) for col in df.columns])]

    # We need to handle null values if they come up https://stackoverflow.com/questions/40057563/replace-missing-values-with-mean-spark-dataframe
    # mean_dict = {col: 'mean' for col in df.columns}
    # col_avgs = df.agg(mean_dict).collect()[0].asDict()
    # col_avgs = {k[4:-1]: v for k, v in col_avgs.iteritems()}
    # df.fillna(col_avgs).show()
    df = df.fillna({ 'score': 0, 'week': 0, 'repo': '' })


    indexer = StringIndexer(inputCol=category,
                             outputCol="{}_indexed".format(category), handleInvalid='skip')

    one_hot_encoder = OneHotEncoder(dropLast=True, inputCol=indexer.getOutputCol(),
                                     outputCol="{}_encoded".format(indexer.getOutputCol()))

    # This steps puts our features in a form that will be understood by the regression models
    features = VectorAssembler(inputCols=[one_hot_encoder.getOutputCol()] + ['week', 'score'],
                                outputCol="features")

    pipeline = Pipeline(stages=[indexer, one_hot_encoder, features])

    model = pipeline.fit(df)
    return model.transform(df)
