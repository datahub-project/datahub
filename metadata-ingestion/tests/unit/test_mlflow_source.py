import unittest
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mlflow import MlFlowSource
from datahub.metadata import MLModelPropertiesClass
from datahub.metadata.com.linkedin.pegasus2avro.common import VersionTag


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

    @patch("datahub.ingestion.source.mlflow.MlFlowSource.get_mlflow_objects")
    def test_mlflow_source_creates_correct_workunits(self, mocked_mlflow_objects):
        # Given:
        config = {
            'tracking_uri': 'localhost:5000'
        }

        experiments_name = [MLModelPropertiesClass(name='first_model', version=VersionTag(versionTag='version1')),
                            MLModelPropertiesClass(name='second_model', version=VersionTag(versionTag='version2'))]

        # When
        ctx = PipelineContext(run_id="test")
        mlflow_source = MlFlowSource.create(config, ctx)

        mocked_mlflow_objects.return_value = experiments_name

        workunits = []
        for w in mlflow_source.get_workunits():
            workunits.append(w)

        # Then
        assert len(workunits) == 2

        first_workunit = workunits[0]
        assert first_workunit.__dict__['mce']['proposedSnapshot'][
                   'urn'] == f'urn:li:mlModel:(urn:li:dataPlatform:mlflow,first_model_version1,PROD)'

        second_workunit = workunits[1]
        assert second_workunit.__dict__['mce']['proposedSnapshot'][
                   'urn'] == f'urn:li:mlModel:(urn:li:dataPlatform:mlflow,second_model_version2,PROD)'
