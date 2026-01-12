import os
import tempfile
from typing import cast

import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.pentaho.config import PentahoSourceConfig
from datahub.ingestion.source.pentaho.pentaho import PentahoSource
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataJobSnapshotClass,
    MetadataChangeEventClass,
)


class TestPentahoSourceIntegration:
    """Integration tests for Pentaho source."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def sample_ktr_content(self):
        """Sample KTR file content for testing."""
        return """<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info>
    <name>test_transformation</name>
    <description>Test transformation for integration testing</description>
    <trans_type>Normal</trans_type>
    <directory>/</directory>
    <created_user>test_user</created_user>
    <created_date>2024/05/29 13:12:13.111</created_date>
  </info>

  <connection>
    <name>mysql_conn</name>
    <server>localhost</server>
    <type>MYSQL</type>
    <access>Native</access>
    <database>test_db</database>
    <port>3306</port>
    <username>test_user</username>
    <password>Encrypted 2be98afc86aa7f2e4cb12b721cec2a28c</password>
  </connection>

  <connection>
    <name>postgres_conn</name>
    <server>pg-server</server>
    <type>POSTGRESQL</type>
    <access>Native</access>
    <database>analytics</database>
    <port>5432</port>
    <username>pg_user</username>
    <password>Encrypted 2be98afc86aa7f2e4cb12b721cec2a28c</password>
  </connection>

  <order>
    <hop><from>Table input</from><to>Table output</to><enabled>Y</enabled></hop>
  </order>

  <step>
    <name>Table input</name>
    <type>TableInput</type>
    <distribute>Y</distribute>
    <copies>1</copies>
    <connection>mysql_conn</connection>
    <table>source_table</table>
    <sql>SELECT * FROM source_table WHERE active = 1</sql>
    <variables_active>N</variables_active>
    <GUI><xloc>192</xloc><yloc>32</yloc></GUI>
  </step>

  <step>
    <name>Table output</name>
    <type>TableOutput</type>
    <distribute>Y</distribute>
    <copies>1</copies>
    <connection>postgres_conn</connection>
    <schema>public</schema>
    <table>target_table</table>
    <commit>1000</commit>
    <use_batch>Y</use_batch>
    <specify_fields>N</specify_fields>
    <GUI><xloc>352</xloc><yloc>32</yloc></GUI>
  </step>
</transformation>"""

    @pytest.fixture
    def sample_ktr_with_variables(self):
        """Sample KTR file with variables that cannot be resolved."""
        return """<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info>
    <name>variable_transformation</name>
    <description>Transformation with variables</description>
    <trans_type>Normal</trans_type>
    <directory>/</directory>
  </info>

  <connection>
    <name>bq_conn</name>
    <server>https://www.googleapis.com/oauth2/v1/</server>
    <type>GOOGLEBIGQUERY</type>
    <access>Native</access>
    <database>test-project</database>
    <port>443</port>
  </connection>

  <step>
    <name>Table input with variables</name>
    <type>TableInput</type>
    <distribute>Y</distribute>
    <copies>1</copies>
    <connection>bq_conn</connection>
    <sql>SELECT * FROM test-project.dataset.${table_name}</sql>
    <variables_active>Y</variables_active>
    <GUI><xloc>192</xloc><yloc>32</yloc></GUI>
  </step>

  <step>
    <name>Table output with variables</name>
    <type>TableOutput</type>
    <distribute>Y</distribute>
    <copies>1</copies>
    <connection>bq_conn</connection>
    <table>${output_table}</table>
    <GUI><xloc>352</xloc><yloc>32</yloc></GUI>
  </step>
</transformation>"""

    @pytest.fixture
    def sample_kjb_content(self):
        """Sample KJB file content for testing."""
        return """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>test_job</name>
  <description>Test job for integration testing</description>
  <created_user>test_user</created_user>
  <created_date>2024/04/21 16:18:13.111</created_date>

  <entries>
    <entry>
      <name>START</name>
      <description/>
      <type>SPECIAL</type>
      <attributes/>
    </entry>

    <entry>
      <name>run_transformation</name>
      <description>Execute transformation</description>
      <type>TRANS</type>
      <attributes>
        <filename>test_transformation.ktr</filename>
      </attributes>
    </entry>

    <entry>
      <name>run_another_job</name>
      <description>Execute another job</description>
      <type>JOB</type>
      <attributes>
        <filename>another_job.kjb</filename>
      </attributes>
    </entry>
  </entries>

  <hops>
    <hop>
      <from>START</from>
      <to>run_transformation</to>
      <enabled>Y</enabled>
    </hop>
    <hop>
      <from>run_transformation</from>
      <to>run_another_job</to>
      <enabled>Y</enabled>
    </hop>
  </hops>
</job>"""

    @pytest.fixture
    def default_config(self, temp_dir):
        """Default configuration for testing."""
        return {
            "base_folder": temp_dir,
            "env": "TEST",
            "default_owner": "test_owner",
            "file_size_limit_mb": 10,
            "platform_instance": "test_instance",
        }

    def create_test_file(self, temp_dir: str, filename: str, content: str) -> str:
        """Helper to create test files."""
        file_path = os.path.join(temp_dir, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        return file_path

    def test_ktr_processing_with_clear_tables(
        self, temp_dir, sample_ktr_content, default_config
    ):
        """Test processing a KTR file with clear table references."""
        # Create test file
        self.create_test_file(temp_dir, "test.ktr", sample_ktr_content)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Get work units
        work_units = list(source.get_workunits())

        # Assertions
        assert len(work_units) == 1

        work_unit = work_units[0]
        assert work_unit.id == "pentaho-ktr-test_transformation"

        # Check snapshot
        snapshot_event = cast(MetadataChangeEventClass, work_unit.metadata)
        snapshot = snapshot_event.proposedSnapshot
        assert snapshot is not None
        assert isinstance(snapshot, DataJobSnapshotClass)
        assert "test_transformation" in snapshot.urn
        assert "pentaho" in snapshot.urn

        # Check aspects
        aspects = snapshot.aspects
        assert (
            len(aspects) >= 4
        )  # Key, Info, Ownership, GlobalTags, potentially InputOutput

        # Find DataJobInfo aspect
        job_info = None
        input_output = None
        for aspect in aspects:
            if isinstance(aspect, DataJobInfoClass):
                job_info = aspect
            elif isinstance(aspect, DataJobInputOutputClass):
                input_output = aspect

        assert job_info is not None
        assert job_info.name == "test_transformation"
        assert job_info.type == "TRANSFORMATION"
        assert job_info.description == "Test transformation for integration testing"

        # Check custom properties
        custom_props = job_info.customProperties
        assert custom_props["source"] == "Pentaho"
        assert custom_props["file_type"] == "transformation"
        assert custom_props["input_count"] == "1"
        assert custom_props["output_count"] == "1"

        # Check lineage
        assert input_output is not None
        assert len(input_output.inputDatasets) == 1
        assert len(input_output.outputDatasets) == 1

        # Verify dataset URNs
        input_urn = input_output.inputDatasets[0]
        output_urn = input_output.outputDatasets[0]

        assert "mysql" in input_urn
        assert "source_table" in input_urn
        assert "TEST" in input_urn

        assert "postgres" in output_urn
        assert "public.target_table" in output_urn
        assert "TEST" in output_urn

    def test_ktr_processing_with_variables(
        self, temp_dir, sample_ktr_with_variables, default_config
    ):
        """Test processing a KTR file with variables (should handle gracefully)."""
        # Create test file
        self.create_test_file(temp_dir, "variable_test.ktr", sample_ktr_with_variables)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Get work units
        work_units = list(source.get_workunits())

        # Should still create work unit even with variables
        assert len(work_units) == 1

        work_unit = work_units[0]
        snapshot_event = cast(MetadataChangeEventClass, work_unit.metadata)
        snapshot = snapshot_event.proposedSnapshot
        assert snapshot is not None

        # Find DataJobInfo aspect
        job_info = None
        input_output = None
        for aspect in snapshot.aspects:
            if isinstance(aspect, DataJobInfoClass):
                job_info = aspect
            elif isinstance(aspect, DataJobInputOutputClass):
                input_output = aspect

        assert job_info is not None
        assert job_info.name == "variable_transformation"

        # Should have lineage even with variables (though URNs may contain variables)
        if input_output:
            # Variables should be preserved in the URN since they can't be resolved
            for dataset_urn in input_output.inputDatasets + input_output.outputDatasets:
                assert "bigquery" in dataset_urn or "unknown" in dataset_urn

    def test_kjb_processing(self, temp_dir, sample_kjb_content, default_config):
        """Test processing a KJB file."""
        # Create test file
        self.create_test_file(temp_dir, "test.kjb", sample_kjb_content)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Get work units
        work_units = list(source.get_workunits())

        # Assertions
        assert len(work_units) == 1

        work_unit = work_units[0]
        assert work_unit.id == "pentaho-kjb-test_job"

        # Check snapshot
        snapshot_event = cast(MetadataChangeEventClass, work_unit.metadata)
        snapshot = snapshot_event.proposedSnapshot
        assert snapshot is not None
        assert isinstance(snapshot, DataJobSnapshotClass)
        assert "test_job" in snapshot.urn

        # Find DataJobInfo aspect
        job_info = None
        for aspect in snapshot.aspects:
            if isinstance(aspect, DataJobInfoClass):
                job_info = aspect

        assert job_info is not None
        assert job_info.name == "test_job"
        assert job_info.type == "JOB"
        assert job_info.description == "Test job for integration testing"

        # Check custom properties for dependencies
        custom_props = job_info.customProperties
        assert custom_props["source"] == "Pentaho"
        assert custom_props["file_type"] == "job"
        assert custom_props["dependency_count"] == "2"
        assert "dependency_1" in custom_props
        assert "dependency_2" in custom_props

    def test_multiple_files_processing(
        self, temp_dir, sample_ktr_content, sample_kjb_content, default_config
    ):
        """Test processing multiple files."""
        # Create multiple test files
        self.create_test_file(temp_dir, "transformation1.ktr", sample_ktr_content)
        self.create_test_file(
            temp_dir,
            "transformation2.ktr",
            sample_ktr_content.replace("test_transformation", "test_transformation2"),
        )
        self.create_test_file(temp_dir, "job1.kjb", sample_kjb_content)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Get work units
        work_units = list(source.get_workunits())

        # Should process all files
        assert len(work_units) == 3

        # Check that we have both transformations and jobs
        transformation_count = 0
        job_count = 0

        for work_unit in work_units:
            if "ktr" in work_unit.id:
                transformation_count += 1
            elif "kjb" in work_unit.id:
                job_count += 1

        assert transformation_count == 2
        assert job_count == 1

    def test_file_size_limit(self, temp_dir, sample_ktr_content, default_config):
        """Test file size limit configuration."""
        # Create config with small file size limit
        config = default_config.copy()
        config["file_size_limit_mb"] = 0.001

        # Create test file
        self.create_test_file(temp_dir, "large_file.ktr", sample_ktr_content)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(config), ctx)

        # Get work units - should be empty due to size limit
        work_units = list(source.get_workunits())
        assert len(work_units) == 0

    def test_platform_mappings(self, temp_dir, default_config):
        """Test platform mapping configuration."""
        # Create KTR with various connection types
        ktr_content = """<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info>
    <name>platform_test</name>
    <description>Test platform mappings</description>
  </info>

  <connection>
    <name>oracle_conn</name>
    <type>ORACLE</type>
  </connection>

  <connection>
    <name>snowflake_conn</name>
    <type>SNOWFLAKE</type>
  </connection>

  <step>
    <name>Oracle input</name>
    <type>TableInput</type>
    <connection>oracle_conn</connection>
    <table>oracle_table</table>
  </step>

  <step>
    <name>Snowflake output</name>
    <type>TableOutput</type>
    <connection>snowflake_conn</connection>
    <table>snowflake_table</table>
  </step>
</transformation>"""

        # Create test file
        self.create_test_file(temp_dir, "platform_test.ktr", ktr_content)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Get work units
        work_units = list(source.get_workunits())
        assert len(work_units) == 1

        # Check platform mappings in URNs
        input_output = None
        snapshot_event = cast(MetadataChangeEventClass, work_units[0].metadata)
        snapshot = snapshot_event.proposedSnapshot
        assert snapshot is not None
        for aspect in snapshot.aspects:
            if isinstance(aspect, DataJobInputOutputClass):
                input_output = aspect

        if input_output:
            input_urn = (
                input_output.inputDatasets[0] if input_output.inputDatasets else ""
            )
            output_urn = (
                input_output.outputDatasets[0] if input_output.outputDatasets else ""
            )

            assert "oracle" in input_urn
            assert "snowflake" in output_urn

    def test_empty_directory(self, temp_dir, default_config):
        """Test processing empty directory."""
        # Create empty subdirectory
        empty_dir = os.path.join(temp_dir, "empty")
        os.makedirs(empty_dir)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Get work units
        work_units = list(source.get_workunits())
        assert len(work_units) == 0

    def test_invalid_xml_file(self, temp_dir, default_config):
        """Test handling of invalid XML files."""
        # Create invalid XML file
        invalid_xml = """<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info>
    <name>invalid_transformation</name>
    <!-- Missing closing tag -->
  </info>
  <step>
    <name>Test step</name>
    <type>TableInput</type>
    <!-- Missing closing transformation tag -->"""

        self.create_test_file(temp_dir, "invalid.ktr", invalid_xml)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Get work units - should handle gracefully
        work_units = list(source.get_workunits())
        assert len(work_units) == 0  # Should skip invalid files

        # Check that failure was reported
        report = source.get_report()
        assert len(report.failures) > 0

    def test_configuration_validation(self, temp_dir):
        """Test configuration validation."""
        # Test with missing required fields
        with pytest.raises(ValidationError):
            PentahoSourceConfig.parse_obj({})

        # Test with valid configuration
        config = PentahoSourceConfig.parse_obj({"base_folder": temp_dir, "env": "TEST"})

        assert config.base_folder == temp_dir
        assert config.env == "TEST"
        assert config.default_owner == "pentaho_admin"  # Default value
        assert config.file_size_limit_mb == 50  # Default value

    def test_report_generation(self, temp_dir, sample_ktr_content, default_config):
        """Test that source report is generated correctly."""
        # Create test file
        self.create_test_file(temp_dir, "test.ktr", sample_ktr_content)

        # Initialize source
        ctx = PipelineContext(run_id="test_run")
        source = PentahoSource(PentahoSourceConfig.parse_obj(default_config), ctx)

        # Process files
        work_units = list(source.get_workunits())

        # Get report
        report = source.get_report()

        # Basic report validation
        assert hasattr(report, "failures")
        assert hasattr(report, "warnings")
        assert len(work_units) > 0  # Should have processed files