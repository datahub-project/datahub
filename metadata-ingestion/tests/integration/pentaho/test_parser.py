import tempfile
from pathlib import Path

import pytest

from datahub.ingestion.source.pentaho.parser import PentahoKettleParser


# Sample transformation content with variables
TRANSFORMATION_WITH_VARIABLES = """<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info>
    <name>Variable Test</name>
    <description>Testing variable resolution</description>
  </info>
  <step>
    <name>Input with variables</name>
    <type>TableInput</type>
    <connection>db_conn</connection>
    <sql>SELECT * FROM ${schema}.${table_name} WHERE date > '${start_date}'</sql>
  </step>
  <step>
    <name>Output with variables</name>
    <type>TableOutput</type>
    <connection>db_conn</connection>
    <schema>${output_schema}</schema>
    <table>${output_table}</table>
  </step>
</transformation>"""

# Sample job content
JOB_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>Test Job</name>
  <description>A test job</description>
  <created_user>admin</created_user>
  <created_date>2023/01/01 12:00:00.000</created_date>
  <entries>
    <entry>
      <name>SQL Task</name>
      <type>SQL</type>
      <sql>CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(50))</sql>
    </entry>
    <entry>
      <name>Transformation Task</name>
      <type>TRANS</type>
    </entry>
  </entries>
</job>"""


@pytest.fixture
def parser_no_variables():
    """Parser without variable resolution"""
    return PentahoKettleParser(resolve_variables=False)


@pytest.fixture
def parser_with_variables():
    """Parser with variable resolution"""
    variable_values = {
        'schema': 'public',
        'table_name': 'source_data',
        'start_date': '2023-01-01',
        'output_schema': 'staging',
        'output_table': 'processed_data'
    }
    return PentahoKettleParser(resolve_variables=True, variable_values=variable_values)


def test_parse_transformation_basic():
    """Test parsing a basic transformation"""
    content = """<?xml version="1.0" encoding="UTF-8"?>
    <transformation>
      <info>
        <name>Basic Transform</name>
        <description>A basic transformation</description>
        <created_user>test_user</created_user>
        <size_rowset>5000</size_rowset>
      </info>
      <step>
        <name>Read Data</name>
        <type>TableInput</type>
        <connection>mysql_conn</connection>
        <sql>SELECT id, name FROM users</sql>
        <execute_each_row>N</execute_each_row>
      </step>
      <step>
        <name>Write Data</name>
        <type>TableOutput</type>
        <connection>postgres_conn</connection>
        <schema>public</schema>
        <table>user_data</table>
        <commit_size>1000</commit_size>
      </step>
    </transformation>"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ktr', delete=False) as f:
        f.write(content)
        f.flush()
        
        parser = PentahoKettleParser()
        job = parser.parse_file(f.name)
        
        assert job is not None
        assert job.name == "Basic Transform"
        assert job.type == "transformation"
        assert job.description == "A basic transformation"
        assert len(job.steps) == 2
        
        # Check TableInput step
        input_step = next(s for s in job.steps if s.type == "TableInput")
        assert input_step.name == "Read Data"
        assert input_step.connection_name == "mysql_conn"
        assert input_step.sql == "SELECT id, name FROM users"
        assert input_step.custom_properties['execute_each_row'] == 'N'
        
        # Check TableOutput step
        output_step = next(s for s in job.steps if s.type == "TableOutput")
        assert output_step.name == "Write Data"
        assert output_step.connection_name == "postgres_conn"
        assert output_step.table_name == "public.user_data"
        assert output_step.custom_properties['commit_size'] == '1000'
        
        # Check job custom properties
        assert job.custom_properties['created_user'] == 'test_user'
        assert job.custom_properties['size_rowset'] == '5000'
    
    Path(f.name).unlink()


def test_parse_job_basic():
    """Test parsing a basic job"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.kjb', delete=False) as f:
        f.write(JOB_CONTENT)
        f.flush()
        
        parser = PentahoKettleParser()
        job = parser.parse_file(f.name)
        
        assert job is not None
        assert job.name == "Test Job"
        assert job.type == "job"
        assert job.description == "A test job"
        assert len(job.steps) == 2
        
        # Check SQL entry
        sql_step = next(s for s in job.steps if s.type == "SQL")
        assert sql_step.name == "SQL Task"
        assert "CREATE TABLE" in sql_step.sql
        
        # Check job custom properties
        assert job.custom_properties['created_user'] == 'admin'
    
    Path(f.name).unlink()


def test_variable_resolution_disabled(parser_no_variables):
    """Test that variables are not resolved when disabled"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ktr', delete=False) as f:
        f.write(TRANSFORMATION_WITH_VARIABLES)
        f.flush()
        
        job = parser_no_variables.parse_file(f.name)
        
        assert job is not None
        input_step = next(s for s in job.steps if s.type == "TableInput")
        
        # Variables should remain unresolved
        assert "${schema}" in input_step.sql
        assert "${table_name}" in input_step.sql
        assert "${start_date}" in input_step.sql
        
        output_step = next(s for s in job.steps if s.type == "TableOutput")
        assert output_step.table_name == "${output_schema}.${output_table}"
    
    Path(f.name).unlink()


def test_variable_resolution_enabled(parser_with_variables):
    """Test that variables are resolved when enabled"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ktr', delete=False) as f:
        f.write(TRANSFORMATION_WITH_VARIABLES)
        f.flush()
        
        job = parser_with_variables.parse_file(f.name)
        
        assert job is not None
        input_step = next(s for s in job.steps if s.type == "TableInput")
        
        # Variables should be resolved
        assert "public.source_data" in input_step.sql
        assert "2023-01-01" in input_step.sql
        
        output_step = next(s for s in job.steps if s.type == "TableOutput")
        assert output_step.table_name == "staging.processed_data"
    
    Path(f.name).unlink()


def test_extract_table_name_from_sql():
    """Test extracting table names from SQL"""
    parser = PentahoKettleParser()
    
    # Simple SELECT
    table_name = parser._extract_table_name_from_sql("SELECT * FROM users")
    assert table_name == "users"
    
    # SELECT with schema
    table_name = parser._extract_table_name_from_sql("SELECT * FROM public.users")
    assert table_name == "public.users"
    
    # SELECT with quotes
    table_name = parser._extract_table_name_from_sql('SELECT * FROM "public"."users"')
    assert table_name == "public.users"
    
    # Complex query
    table_name = parser._extract_table_name_from_sql(
        "SELECT u.id, u.name FROM users u WHERE u.active = 1"
    )
    assert table_name == "users"
    
    # No table found
    table_name = parser._extract_table_name_from_sql("SELECT 1")
    assert table_name is None


def test_parse_nonexistent_file():
    """Test parsing a file that doesn't exist"""
    parser = PentahoKettleParser()
    job = parser.parse_file("/nonexistent/file.ktr")
    assert job is None


def test_parse_invalid_xml():
    """Test parsing invalid XML"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ktr', delete=False) as f:
        f.write("This is not valid XML")
        f.flush()
        
        parser = PentahoKettleParser()
        job = parser.parse_file(f.name)
        assert job is None
    
    Path(f.name).unlink()


def test_parse_unsupported_file_type():
    """Test parsing unsupported file type"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("Some content")
        f.flush()
        
        parser = PentahoKettleParser()
        job = parser.parse_file(f.name)
        assert job is None
    
    Path(f.name).unlink()


def test_get_text_helper():
    """Test the _get_text helper method"""
    import xml.etree.ElementTree as ET
    
    parser = PentahoKettleParser()
    
    # Test with valid element
    root = ET.fromstring("<root><child>value</child></root>")
    assert parser._get_text(root, 'child') == 'value'
    
    # Test with missing element
    assert parser._get_text(root, 'missing') == ''
    assert parser._get_text(root, 'missing', 'default') == 'default'
    
    # Test with None element
    assert parser._get_text(None, 'child') == ''
    assert parser._get_text(None, 'child', 'default') == 'default'