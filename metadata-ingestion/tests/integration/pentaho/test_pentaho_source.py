import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.pentaho.config import PentahoSourceConfig
from datahub.ingestion.source.pentaho.source import PentahoSource


# Sample Pentaho transformation (.ktr) file content
SAMPLE_KTR_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info>
    <name>Sample Transformation</name>
    <description>A sample transformation for testing</description>
    <created_user>test_user</created_user>
    <created_date>2023/01/01 12:00:00.000</created_date>
    <modified_user>test_user</modified_user>
    <modified_date>2023/01/01 12:00:00.000</modified_date>
    <size_rowset>10000</size_rowset>
  </info>
  <step>
    <name>Table input</name>
    <type>TableInput</type>
    <connection>mysql_conn</connection>
    <sql>SELECT * FROM source_table WHERE id > 100</sql>
    <execute_each_row>N</execute_each_row>
    <variables_active>N</variables_active>
    <lazy_conversion_active>N</lazy_conversion_active>
  </step>
  <step>
    <name>Table output</name>
    <type>TableOutput</type>
    <connection>postgres_conn</connection>
    <schema>public</schema>
    <table>destination_table</table>
    <commit_size>1000</commit_size>
    <truncate>N</truncate>
    <ignore_errors>N</ignore_errors>
    <use_batch>Y</use_batch>
  </step>
</transformation>"""

# Sample Pentaho job (.kjb) file content
SAMPLE_KJB_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>Sample Job</name>
  <description>A sample job for testing</description>
  <created_user>test_user</created_user>
  <created_date>2023/01/01 12:00:00.000</created_date>
  <modified_user>test_user</modified_user>
  <modified_date>2023/01/01 12:00:00.000</modified_date>
  <entries>
    <entry>
      <name>SQL Script</name>
      <type>SQL</type>
      <sql>INSERT INTO log_table VALUES ('Job started')</sql>
    </entry>
  </entries>
</job>"""


@pytest.fixture
def temp_ktr_file():
    """Create a temporary .ktr file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ktr', delete=False) as f:
        f.write(SAMPLE_KTR_CONTENT)
        f.flush()
        yield Path(f.name)
    Path(f.name).unlink()


@pytest.fixture
def temp_kjb_file():
    """Create a temporary .kjb file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.kjb', delete=False) as f:
        f.write(SAMPLE_KJB_CONTENT)
        f.flush()
        yield Path(f.name)
    Path(f.name).unlink()


@pytest.fixture
def pentaho_config(temp_ktr_file, temp_kjb_file):
    """Create a PentahoSourceConfig for testing"""
    return PentahoSourceConfig(
        kettle_file_paths=[str(temp_ktr_file), str(temp_kjb_file)],
        database_mapping={
            'mysql_conn': 'mysql',
            'postgres_conn': 'postgres'
        },
        default_database_platform='unknown',
        include_lineage=True,
        include_job_metadata=True,
    )


@pytest.fixture
def pentaho_source(pentaho_config):
    """Create a PentahoSource for testing"""
    ctx = PipelineContext(run_id="test_run")
    return PentahoSource(pentaho_config, ctx)


def test_pentaho_source_init(pentaho_source):
    """Test PentahoSource initialization"""
    assert pentaho_source.platform == "pentaho"
    assert pentaho_source.config.include_lineage is True
    assert pentaho_source.config.include_job_metadata is True


def test_find_kettle_files(pentaho_source, temp_ktr_file, temp_kjb_file):
    """Test finding Kettle files"""
    files = pentaho_source._find_kettle_files()
    
    # Should find both files
    assert len(files) == 2
    file_paths = [str(f) for f in files]
    assert str(temp_ktr_file) in file_paths
    assert str(temp_kjb_file) in file_paths


def test_process_ktr_file(pentaho_source, temp_ktr_file):
    """Test processing a .ktr file"""
    workunits = list(pentaho_source._process_kettle_file(temp_ktr_file))
    
    # Should generate work units for the job
    assert len(workunits) > 0
    
    # Check that file was processed successfully
    assert pentaho_source.report.files_processed == 1
    assert pentaho_source.report.jobs_processed == 1
    assert pentaho_source.report.jobs_with_lineage == 1


def test_process_kjb_file(pentaho_source, temp_kjb_file):
    """Test processing a .kjb file"""
    workunits = list(pentaho_source._process_kettle_file(temp_kjb_file))
    
    # Should generate work units for the job
    assert len(workunits) > 0
    
    # Check that file was processed successfully
    assert pentaho_source.report.files_processed == 1
    assert pentaho_source.report.jobs_processed == 1


def test_get_workunits_internal(pentaho_source):
    """Test the main get_workunits_internal method"""
    workunits = list(pentaho_source.get_workunits_internal())
    
    # Should process both files and generate work units
    assert len(workunits) > 0
    assert pentaho_source.report.files_processed == 2
    assert pentaho_source.report.jobs_processed == 2


def test_config_validation():
    """Test configuration validation"""
    # Test valid config
    config = PentahoSourceConfig(kettle_file_paths=["/path/to/file.ktr"])
    assert config.kettle_file_paths == ["/path/to/file.ktr"]
    
    # Test empty file paths should raise error
    with pytest.raises(ValueError, match="At least one kettle file path must be specified"):
        PentahoSourceConfig(kettle_file_paths=[])


def test_database_mapping_normalization():
    """Test that database mapping values are normalized to lowercase"""
    config = PentahoSourceConfig(
        kettle_file_paths=["/path/to/file.ktr"],
        database_mapping={'MYSQL_CONN': 'MySQL', 'postgres_conn': 'PostgreSQL'}
    )
    
    # Values should be lowercase
    assert config.database_mapping == {'MYSQL_CONN': 'mysql', 'postgres_conn': 'postgresql'}


@patch('datahub.ingestion.source.pentaho.source.glob.glob')
def test_no_files_found(mock_glob, pentaho_config):
    """Test behavior when no files are found"""
    mock_glob.return_value = []
    
    ctx = PipelineContext(run_id="test_run")
    source = PentahoSource(pentaho_config, ctx)
    
    workunits = list(source.get_workunits_internal())
    assert len(workunits) == 0


def test_file_pattern_filtering(temp_ktr_file):
    """Test file pattern filtering"""
    from datahub.configuration.common import AllowDenyPattern
    
    config = PentahoSourceConfig(
        kettle_file_paths=[str(temp_ktr_file)],
        file_pattern=AllowDenyPattern(deny=["*.ktr"])  # Deny .ktr files
    )
    
    ctx = PipelineContext(run_id="test_run")
    source = PentahoSource(config, ctx)
    
    files = source._find_kettle_files()
    assert len(files) == 0  # File should be filtered out
    assert source.report.files_skipped == 1