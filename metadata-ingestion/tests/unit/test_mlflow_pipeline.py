import unittest
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline


class MlFlowPipelineTest(unittest.TestCase):
    @patch("datahub.ingestion.source.mlflow.MlFlowSource.get_workunits")
    @patch("datahub.ingestion.sink.console.ConsoleSink.close")
    def test_configure(self, mock_sink, mock_source):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "mlflow",
                    "config": {"tracking_uri": "localhost:5000"},
                },
                "sink": {"type": "console"},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        mock_source.assert_called_once()
        mock_sink.assert_called_once()
