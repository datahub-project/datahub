import unittest

import mlflow
import numpy as np
import pandas as pd
import pytest
from datahub.ingestion.run.pipeline import Pipeline
from sklearn import datasets
from sklearn.dummy import DummyRegressor
from sklearn.model_selection import train_test_split

from test_helpers.mce_helpers import load_json_file, assert_mces_equal

TRACKING_URI = 'http://localhost:5000'


EXPERIMENT_NAME = 'dummy_experiment'


@pytest.fixture
def setup_mlflow_client():
    mlflow_client = mlflow.tracking.MlflowClient(TRACKING_URI)
    first_experiment_id = mlflow_client.create_experiment(name=EXPERIMENT_NAME)
    mlflow.set_experiment(EXPERIMENT_NAME)
    yield mlflow_client, first_experiment_id
    mlflow_client.delete_experiment(first_experiment_id)


def test_mlflow_ingests_multiple_mlflow_experiments_successfully(setup_mlflow_client):
    # Given
    setup_mlflow_client()
    model = DummyRegressor()

    diabetes = datasets.load_diabetes()
    X = diabetes.data
    y = diabetes.target

    Y = np.array([y]).transpose()
    d = np.concatenate((X, Y), axis=1)
    cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6', 'progression']
    data = pd.DataFrame(d, columns=cols)

    train, test = train_test_split(data, test_size=0.2)
    train_x = train.drop(["progression"], axis=1)
    train_y = train[["progression"]]

    tags = {'model_type': 'lr_baseline'}

    with mlflow.start_run():
        model.fit(train_x, train_y)
        mlflow.log_metric("mae", 123)
        mlflow.sklearn.log_model(model, "model")
        mlflow.set_tags(tags)

    recipient = {
        "source": {
            "type": "mlflow",
            "config": {
                "tracking_uri": TRACKING_URI
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": "./mlflow_mce.json"
            }
        },
    }

    pipeline = Pipeline.create(recipient)

    # When:
    pipeline.run()
    pipeline.raise_from_status()
    status = pipeline.pretty_print_summary()
    output_mce = load_json_file(filename="./mlflow_mce.json")

    # Then
    assert status == 0
    assert len(list(output_mce)) == 1
