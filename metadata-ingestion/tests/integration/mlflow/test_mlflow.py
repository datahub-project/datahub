import shutil
import unittest
from typing import List

import mlflow
from datahub.ingestion.run.pipeline import Pipeline

from tests.test_helpers.mce_helpers import load_json_file, assert_mces_equal


def delete_mlflow_experiments(tracking_uri: str, experiments_to_delete: List[str]):
    mlflow_client = mlflow.tracking.MlflowClient(tracking_uri)
    for experiment in experiments_to_delete:
        mlflow_client.delete_experiment(experiment)
    shutil.rmtree(f'./{tracking_uri}/.trash')


class MlFlowTest(unittest.TestCase):
    def test_mlflow_ingests_multiple_mlflow_experiments_successfully(self):
        # Given:
        tracking_uri = 'localhost:5000'
        mlflow_client = mlflow.tracking.MlflowClient(tracking_uri)
        first_experiment_id = mlflow_client.create_experiment(name='first_experiment')
        second_experiment_id = mlflow_client.create_experiment(name='second_experiment')

        golden_mce = load_json_file(filename="./mlflow_golden_mce.json")

        recipient = {
            "source": {
                "type": "mlflow",
                "config": {
                    "tracking_uri": "localhost:5000"
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

        delete_mlflow_experiments(tracking_uri, [first_experiment_id, second_experiment_id])

        # Then
        assert status == 0
        assert_mces_equal(output_mce, golden_mce)
