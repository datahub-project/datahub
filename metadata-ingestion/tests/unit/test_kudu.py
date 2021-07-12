import unittest
from unittest.mock import MagicMock, patch, Mock
from typing import Dict
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kudu import KuduSource, KuduConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
# from mock_alchemy.mocking import AlchemyMagicMock


class KuduSourceTest(unittest.TestCase):
    
    def test_kudu_source_configuration(self):
        """
        Test if the config is accepted without throwing an exception
        """
        ctx = PipelineContext(run_id="test")
        kudu_source = KuduSource.create(
            KuduConfig(use_ssl = False), ctx
        )
        
        assert kudu_source.config.get_sql_alchemy_url() == "impala://localhost:21050/default"

    # @patch('datahub.ingestion.source.kudu.KuduSource.some_random_function', autospec=True)
    # def test_kudu_source_workunits2(self, func):
    #     """
    #     test that the ingestion is able to pull out a workunit 
    #     """
    #     func.return_value=5
    #     ctx = PipelineContext(run_id="test")
    #     a = KuduSource(KuduConfig(use_ssl = False),ctx)
    #     c = KuduSource.random_test(a,func)
    #     self.assertEqual(func.call_count, 2)
    #     self.assertEqual(c, 10)
    
    # @patch('datahub.ingestion.source.kudu.create_engine', autospec=True)
    def test_kudu_source_workunits(self):
        """
        test that the ingestion is able to pull out a workunit 
        """        
        # mock_execute = Mock()
        # engine_attrs = {'execute.return_value': "a"}
        # mock_execute.configure_mock(**engine_attrs)
        func = Mock()
        func.execute.return_value.fetchone.return_value = 'a'

        d = func.execute('select a from b').fetchone()
        self.assertEqual(d, "a")
        # ctx = PipelineContext(run_id="test")
        # a = KuduSource(KuduConfig(use_ssl = False),ctx)
        # c = KuduSource.some_random_function(a,func)
        # func.assert_has_calls()
        
        
        
    
    # def test_kafka_source_workunits_topic_pattern(self, mock_kafka):
    #     mock_kafka_instance = mock_kafka.return_value
    #     mock_cluster_metadata = MagicMock()
    #     mock_cluster_metadata.topics = ["test", "foobar", "bazbaz"]
    #     mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    #     ctx = PipelineContext(run_id="test1")
    #     kafka_source = KuduSource.create(
    #         {
    #             "topic_patterns": {"allow": ["test"]},
    #             "connection": {"bootstrap": "localhost:9092"},
    #         },
    #         ctx,
    #     )
    #     workunits = [w for w in kafka_source.get_workunits()]

    #     mock_kafka.assert_called_once()
    #     mock_kafka_instance.list_topics.assert_called_once()
    #     assert len(workunits) == 1

    #     mock_cluster_metadata.topics = ["test", "test2", "bazbaz"]
    #     ctx = PipelineContext(run_id="test2")
    #     kafka_source = KuduSource.create(
    #         {
    #             "topic_patterns": {"allow": ["test.*"]},
    #             "connection": {"bootstrap": "localhost:9092"},
    #         },
    #         ctx,
    #     )
    #     workunits = [w for w in kafka_source.get_workunits()]
    #     assert len(workunits) == 2

    
    # def test_close(self, mock_kafka):
    #     mock_kafka_instance = mock_kafka.return_value
    #     ctx = PipelineContext(run_id="test")
    #     kafka_source = KuduSource.create(
    #         {
    #             "topic_patterns": {"allow": ["test.*"]},
    #             "connection": {"bootstrap": "localhost:9092"},
    #         },
    #         ctx,
    #     )
    #     kafka_source.close()
    #     assert mock_kafka_instance.close.call_count == 1
