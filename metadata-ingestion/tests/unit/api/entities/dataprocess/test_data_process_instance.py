import time
import unittest
from unittest.mock import Mock

import datahub.metadata.schema_classes as models
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import RunResultType
from datahub.metadata.schema_classes import (
    DataProcessRunStatusClass,
    DataProcessTypeClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DataPlatformUrn, DatasetUrn


class TestDataProcessInstance(unittest.TestCase):
    def setUp(self):
        # Common test data
        self.test_id = "test_process_123"
        self.test_orchestrator = "airflow"
        self.test_cluster = "prod"

        # Create mock ContainerKey
        self.mock_container_key = ContainerKey(
            platform="urn:li:dataPlatform:mlflow", name="test_experiment", env="PROD"
        )

        # Create mock DataJob
        self.mock_flow_urn = DataFlowUrn.create_from_ids(
            orchestrator="airflow", flow_id="test_flow", env="prod"
        )
        self.mock_job_urn = DataJobUrn.create_from_ids(
            job_id="test_job", data_flow_urn=str(self.mock_flow_urn)
        )
        self.mock_datajob = DataJob(
            id="test_job",
            flow_urn=self.mock_flow_urn,
            inlets=[
                DatasetUrn.from_string(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,test_input,PROD)"
                )
            ],
            outlets=[
                DatasetUrn.from_string(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,test_output,PROD)"
                )
            ],
        )

        # Create mock DataFlow
        self.mock_dataflow = DataFlow(
            orchestrator="airflow", id="test_flow", env="prod"
        )

    def test_basic_initialization(self):
        """Test basic initialization of DataProcessInstance"""

        instance = DataProcessInstance(
            id=self.test_id,
            orchestrator=self.test_orchestrator,
            cluster=self.test_cluster,
        )

        self.assertEqual(instance.id, self.test_id)
        self.assertEqual(instance.orchestrator, self.test_orchestrator)
        self.assertEqual(instance.cluster, self.test_cluster)
        self.assertEqual(instance.type, DataProcessTypeClass.BATCH_SCHEDULED)

    def test_from_datajob_creation(self):
        """Test creation of DataProcessInstance from DataJob"""

        instance = DataProcessInstance.from_datajob(
            datajob=self.mock_datajob,
            id=self.test_id,
            clone_inlets=True,
            clone_outlets=True,
        )

        self.assertEqual(instance.id, self.test_id)
        self.assertEqual(instance.orchestrator, "airflow")
        self.assertEqual(instance.template_urn, self.mock_datajob.urn)
        self.assertEqual(len(instance.inlets), 1)
        self.assertEqual(len(instance.outlets), 1)

    def test_from_dataflow_creation(self):
        """Test creation of DataProcessInstance from DataFlow"""

        instance = DataProcessInstance.from_dataflow(
            dataflow=self.mock_dataflow, id=self.test_id
        )

        self.assertEqual(instance.id, self.test_id)
        self.assertEqual(instance.orchestrator, "airflow")
        self.assertEqual(instance.template_urn, self.mock_dataflow.urn)

    def test_from_container_creation(self):
        """Test creation of DataProcessInstance from ContainerKey"""

        instance = DataProcessInstance.from_container(
            container_key=self.mock_container_key, id=self.test_id
        )

        self.assertEqual(instance.id, self.test_id)
        self.assertEqual(instance.orchestrator, "mlflow")  # Platform name from URN
        self.assertIsNone(
            instance.template_urn
        )  # Should be None for container-based instances
        self.assertEqual(instance.container_urn, self.mock_container_key.as_urn())

        # Verify the platform is set correctly
        expected_platform = str(
            DataPlatformUrn.from_string(self.mock_container_key.platform)
        )
        self.assertEqual(instance._platform, expected_platform)

    def test_start_event_generation(self):
        """Test generation of start event MCPs"""

        instance = DataProcessInstance(
            id=self.test_id, orchestrator=self.test_orchestrator
        )

        start_time = int(time.time() * 1000)
        mcps = list(instance.start_event_mcp(start_time, attempt=1))

        self.assertEqual(len(mcps), 1)
        start_event = mcps[0]
        assert isinstance(start_event.aspect, models.DataProcessInstanceRunEventClass)
        self.assertEqual(start_event.aspect.status, DataProcessRunStatusClass.STARTED)
        self.assertEqual(start_event.aspect.timestampMillis, start_time)
        self.assertEqual(start_event.aspect.attempt, 1)

    def test_end_event_generation(self):
        """Test generation of end event MCPs"""

        instance = DataProcessInstance(
            id=self.test_id, orchestrator=self.test_orchestrator
        )

        end_time = int(time.time() * 1000)
        mcps = list(
            instance.end_event_mcp(
                end_time, result=InstanceRunResult.SUCCESS, attempt=1
            )
        )

        self.assertEqual(len(mcps), 1)
        end_event = mcps[0]
        assert isinstance(end_event.aspect, models.DataProcessInstanceRunEventClass)
        self.assertEqual(end_event.aspect.status, DataProcessRunStatusClass.COMPLETE)
        self.assertEqual(end_event.aspect.timestampMillis, end_time)
        assert end_event.aspect.result is not None
        self.assertEqual(end_event.aspect.result.type, RunResultType.SUCCESS)

    def test_emit_process_with_emitter(self):
        """Test emitting process events with mock emitter"""

        mock_emitter = Mock()
        instance = DataProcessInstance(
            id=self.test_id, orchestrator=self.test_orchestrator
        )

        # Test emit method
        instance.emit(mock_emitter)
        self.assertTrue(mock_emitter.emit.called)

        # Test emit_process_start
        start_time = int(time.time() * 1000)
        instance.emit_process_start(mock_emitter, start_time)
        self.assertTrue(mock_emitter.emit.called)

        # Test emit_process_end
        end_time = int(time.time() * 1000)
        instance.emit_process_end(
            mock_emitter, end_time, result=InstanceRunResult.SUCCESS
        )
        self.assertTrue(mock_emitter.emit.called)

    def test_generate_mcp(self):
        """Test generation of MCPs"""

        instance = DataProcessInstance(
            id=self.test_id,
            orchestrator=self.test_orchestrator,
            properties={"env": "prod"},
            url="http://test.url",
        )

        created_time = int(time.time() * 1000)
        mcps = list(instance.generate_mcp(created_time, materialize_iolets=True))

        # Check if we have the basic MCPs generated
        self.assertGreaterEqual(
            len(mcps), 2
        )  # Should at least have properties and relationships

        # Verify the properties MCP
        properties_mcp = next(
            mcp for mcp in mcps if hasattr(mcp.aspect, "customProperties")
        )
        assert isinstance(
            properties_mcp.aspect, models.DataProcessInstancePropertiesClass
        )
        self.assertEqual(properties_mcp.aspect.name, self.test_id)
        self.assertEqual(properties_mcp.aspect.customProperties["env"], "prod")
        self.assertEqual(properties_mcp.aspect.externalUrl, "http://test.url")


if __name__ == "__main__":
    unittest.main()
