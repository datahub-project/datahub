from gometa.ingestion.api.common import PipelineContext
from gometa.ingestion.source.kafka import KafkaSource

from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

import unittest
from unittest.mock import patch, MagicMock


class KafkaSourceTest(unittest.TestCase):

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_kafka_source_configuration(self, mock_kafka):
        ctx = PipelineContext(run_id='test')
        kafka_source = KafkaSource.create({'connection': {'bootstrap': 'foobar'}}, ctx)
        assert mock_kafka.call_count == 1

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_kafka_source_workunits_wildcard_topic(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["foobar", "bazbaz"]
        mock_kafka_instance.list_topics.return_value=mock_cluster_metadata

        ctx = PipelineContext(run_id='test')
        kafka_source = KafkaSource.create({'connection': {'bootstrap': 'localhost:9092'}}, ctx)
        workunits = []
        for w in kafka_source.get_workunits():
            workunits.append(w)

        first_mce = workunits[0].get_metadata()['mce']
        assert isinstance(first_mce, MetadataChangeEvent)
        mock_kafka.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 2

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_kafka_source_workunits_topic_pattern(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["test", "foobar", "bazbaz"]
        mock_kafka_instance.list_topics.return_value=mock_cluster_metadata

        ctx = PipelineContext(run_id='test1')
        kafka_source = KafkaSource.create({'topic': 'test', 'connection': {'bootstrap': 'localhost:9092'}}, ctx)
        assert kafka_source.source_config.topic == "test"
        workunits = [w for w in kafka_source.get_workunits()]

        mock_kafka.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 1

        mock_cluster_metadata.topics = ["test", "test2", "bazbaz"]
        ctx = PipelineContext(run_id='test2')
        kafka_source = KafkaSource.create({'topic': 'test.*', 'connection': {'bootstrap': 'localhost:9092'}}, ctx)
        workunits = [w for w in kafka_source.get_workunits()]
        assert len(workunits) == 2

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_close(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        ctx = PipelineContext(run_id='test')
        kafka_source = KafkaSource.create({'topic': 'test', 'connection': {'bootstrap': 'localhost:9092'}}, ctx)
        kafka_source.close()
        assert mock_kafka_instance.close.call_count == 1
    
