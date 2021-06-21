import unittest
from unittest.mock import patch

import mlflow
from mlflow.entities import Experiment

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mlflow import MlFlowSource


class MlFlowSourceTest(unittest.TestCase):
    def test_mlflow_source_configuration(self):
        # Given:
        config = {
            'tracking_uri': 'localhost:5000',
        }

        expected_mlflow_client_tracking_uri = config['tracking_uri']

        # When:
        ctx = PipelineContext(run_id="test")
        mlflow_source = MlFlowSource.create(config, ctx)

        actual_mlflow_client_tracking_uri = mlflow_source.__dict__['mlflow_client']._registry_uri

        # Then:
        assert actual_mlflow_client_tracking_uri == expected_mlflow_client_tracking_uri

    @patch("datahub.ingestion.source.mlflow.mlflow.tracking.MlflowClient.list_experiments")
    def test_get_mlflow_objects_returns_correct_experiments_from_mlflow_server(self, mocked_mlflow):
        # Given
        mlflow_client = mlflow.tracking.MlflowClient(tracking_uri='localhost:5000')

        mocked_mlflow.return_value = [
            Experiment(name='first_experiment', artifact_location='mock_1', experiment_id=1, lifecycle_stage=''),
            Experiment(name='second_experiment', artifact_location='mock_2', experiment_id=2, lifecycle_stage='')]

        # When
        mlflow_objects = MlFlowSource.get_mlflow_objects(mlflow_client)

        # Then
        assert mlflow_objects == ['first_experiment', 'second_experiment']

    @patch("datahub.ingestion.source.mlflow.MlFlowSource.get_mlflow_objects")
    def test_mlflow_source_creates_correct_workunits(self, mocked_mlflo_objects):
        # Given:
        config = {
            'tracking_uri': 'localhost:5000'
        }

        experiments_name = ['Default', 'first_experiment', 'second_experiment']

        # When
        ctx = PipelineContext(run_id="test")
        mlflow_source = MlFlowSource.create(config, ctx)

        mocked_mlflo_objects.return_value = experiments_name

        workunits = []
        for w in mlflow_source.get_workunits():
            workunits.append(w)

        # Then
        assert len(workunits) == 2

        first_workunit = workunits[0]
        assert first_workunit.__dict__['mce']['proposedSnapshot'][
                   'urn'] == f'urn:li:mlModel:(urn:li:dataPlatform:mlflow,{experiments_name[1]},PROD)'

        second_workunit = workunits[1]
        assert second_workunit.__dict__['mce']['proposedSnapshot'][
                   'urn'] == f'urn:li:mlModel:(urn:li:dataPlatform:mlflow,{experiments_name[2]},PROD)'
