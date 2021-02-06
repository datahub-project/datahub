import unittest
from unittest.mock import patch, MagicMock

from gometa.ingestion.run.pipeline import Pipeline


class PipelineTest(unittest.TestCase):
    @patch("gometa.ingestion.extractor.kafka.KafkaMetadataExtractor")
    @patch("gometa.ingestion.source.kafka.KafkaSource.get_workunits")
    @patch("gometa.ingestion.sink.console.ConsoleSink.close")
    def test_configure(self, mock_sink, mock_source, mock_extractor):
        pipeline = Pipeline(
            {
                "source": {
                    "type": "kafka",
                    "extractor": "gometa.ingestion.extractor.kafka.KafkaMetadataExtractor",
                    "kafka": {"bootstrap": "localhost:9092"},
                },
                "sink": {"type": "console"},
            }
        )
        pipeline.run()
        mock_source.assert_called_once()
        mock_sink.assert_called_once()

