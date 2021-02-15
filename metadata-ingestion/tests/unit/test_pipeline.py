import unittest
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline


class PipelineTest(unittest.TestCase):
    @patch("datahub.ingestion.source.kafka.KafkaSource.get_workunits")
    @patch("datahub.ingestion.sink.console.ConsoleSink.close")
    def test_configure(self, mock_sink, mock_source):
        pipeline = Pipeline.create(
            {
                "source": {"type": "kafka", "config": {"bootstrap": "localhost:9092"}},
                "sink": {"type": "console"},
            }
        )
        pipeline.run()
        mock_source.assert_called_once()
        mock_sink.assert_called_once()
