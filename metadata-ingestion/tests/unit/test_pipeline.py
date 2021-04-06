import unittest
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline


class PipelineTest(unittest.TestCase):
    @patch("datahub.ingestion.source.kafka.KafkaSource.get_workunits", autospec=True)
    @patch("datahub.ingestion.sink.console.ConsoleSink.close", autospec=True)
    def test_configure(self, mock_sink, mock_source):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "kafka",
                    "config": {"connection": {"bootstrap": "localhost:9092"}},
                },
                "sink": {"type": "console"},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        mock_source.assert_called_once()
        mock_sink.assert_called_once()
