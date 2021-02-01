from gometa.ingestion.source.kafka import KafkaSource

import unittest
from unittest.mock import patch, MagicMock


class KafkaSourceTest(unittest.TestCase):

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_kafka_source_configuration(self, mock_kafka):
        kafka_source = KafkaSource()
        kafka_source.configure({'connection': {'bootstrap': 'foobar'}})
        assert mock_kafka.call_count == 1
        kafka_source.configure({'topic': 'foobar'})

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_kafka_source_workunits_wildcard_topic(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["foobar", "bazbaz"]
        mock_kafka_instance.list_topics.return_value=mock_cluster_metadata

        kafka_source = KafkaSource().configure({'connection': {'bootstrap': 'localhost:9092'}})
        workunits = []
        for w in kafka_source.get_workunits():
            workunits.append(w)

        assert workunits[0].get_metadata()['topic'] == 'foobar'
        mock_kafka.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 2

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_kafka_source_workunits_topic_pattern(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["test", "foobar", "bazbaz"]
        mock_kafka_instance.list_topics.return_value=mock_cluster_metadata

        kafka_source = KafkaSource().configure({'topic': 'test', 'connection': {'bootstrap': 'localhost:9092'}})
        assert kafka_source.source_config.topic == "test"
        workunits = [w for w in kafka_source.get_workunits()]

        mock_kafka.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 1

        mock_cluster_metadata.topics = ["test", "test2", "bazbaz"]
        kafka_source.configure({'topic': 'test.*', 'connection': {'bootstrap': 'localhost:9092'}})
        workunits = [w for w in kafka_source.get_workunits()]
        assert len(workunits) == 2

    @patch("gometa.ingestion.source.kafka.confluent_kafka.Consumer")
    def test_close(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        kafka_source = KafkaSource().configure({'topic': 'test', 'connection': {'bootstrap': 'localhost:9092'}})
        kafka_source.close()
        assert mock_kafka_instance.close.call_count == 1
    
