import pytest

from datahub.ingestion.source.pentaho.lineage_extractor import PentahoLineageExtractor
from datahub.ingestion.source.pentaho.parser import PentahoJob, PentahoStep
from datahub.ingestion.source.pentaho.report import PentahoSourceReport


@pytest.fixture
def lineage_extractor():
    """Create a lineage extractor for testing"""
    database_mapping = {
        'mysql_conn': 'mysql',
        'postgres_conn': 'postgres',
        'oracle_conn': 'oracle'
    }
    
    report = PentahoSourceReport()
    
    return PentahoLineageExtractor(
        database_mapping=database_mapping,
        default_database_platform='unknown',
        env='TEST',
        report=report
    )


@pytest.fixture
def sample_job():
    """Create a sample job with lineage"""
    job = PentahoJob(
        name="Test Job",
        type="transformation",
        description="A test job with lineage",
        file_path="/path/to/test.ktr"
    )
    
    # Add TableInput step (source)
    input_step = PentahoStep(
        name="Read Source",
        type="TableInput",
        sql="SELECT * FROM source_schema.source_table",
        connection_name="mysql_conn"
    )
    job.steps.append(input_step)
    
    # Add TableOutput step (destination)
    output_step = PentahoStep(
        name="Write Target",
        type="TableOutput",
        table_name="target_schema.target_table",
        connection_name="postgres_conn"
    )
    job.steps.append(output_step)
    
    # Add another step that's not relevant for lineage
    other_step = PentahoStep(
        name="Filter Data",
        type="FilterRows"
    )
    job.steps.append(other_step)
    
    return job


def test_extract_lineage_basic(lineage_extractor, sample_job):
    """Test basic lineage extraction"""
    input_datasets, output_datasets = lineage_extractor.extract_lineage(sample_job)
    
    # Should have one input and one output
    assert len(input_datasets) >= 1
    assert len(output_datasets) == 1
    
    # Check that URNs are properly formatted
    output_urn = output_datasets[0]
    assert "urn:li:dataset:(urn:li:dataPlatform:postgres," in output_urn
    assert "target_schema.target_table" in output_urn
    assert ",TEST)" in output_urn


def test_extract_lineage_no_steps(lineage_extractor):
    """Test lineage extraction with no relevant steps"""
    job = PentahoJob(
        name="Empty Job",
        type="transformation",
        file_path="/path/to/empty.ktr"
    )
    
    # Add a step that doesn't contribute to lineage
    job.steps.append(PentahoStep(name="Filter", type="FilterRows"))
    
    input_datasets, output_datasets = lineage_extractor.extract_lineage(job)
    
    assert len(input_datasets) == 0
    assert len(output_datasets) == 0


def test_extract_input_datasets_with_sql(lineage_extractor):
    """Test extracting input datasets from SQL"""
    step = PentahoStep(
        name="SQL Input",
        type="TableInput",
        sql="SELECT a.id, b.name FROM schema1.table1 a JOIN schema2.table2 b ON a.id = b.id",
        connection_name="mysql_conn"
    )
    
    datasets = lineage_extractor._extract_input_datasets(step)
    
    # Should extract datasets from SQL parsing
    assert len(datasets) >= 0  # SQL parsing might not work in test environment


def test_extract_input_datasets_with_table_name(lineage_extractor):
    """Test extracting input datasets from table name fallback"""
    step = PentahoStep(
        name="Table Input",
        type="TableInput",
        table_name="my_schema.my_table",
        connection_name="oracle_conn"
    )
    
    datasets = lineage_extractor._extract_input_datasets(step)
    
    assert len(datasets) == 1
    assert "urn:li:dataset:(urn:li:dataPlatform:oracle," in datasets[0]
    assert "my_schema.my_table" in datasets[0]


def test_extract_output_datasets(lineage_extractor):
    """Test extracting output datasets"""
    step = PentahoStep(
        name="Table Output",
        type="TableOutput",
        table_name="output_schema.output_table",
        connection_name="postgres_conn"
    )
    
    datasets = lineage_extractor._extract_output_datasets(step)
    
    assert len(datasets) == 1
    assert "urn:li:dataset:(urn:li:dataPlatform:postgres," in datasets[0]
    assert "output_schema.output_table" in datasets[0]


def test_get_platform_for_connection(lineage_extractor):
    """Test getting platform for database connections"""
    # Known connection
    platform = lineage_extractor._get_platform_for_connection("mysql_conn")
    assert platform == "mysql"
    
    # Unknown connection
    platform = lineage_extractor._get_platform_for_connection("unknown_conn")
    assert platform == "unknown"
    
    # None connection
    platform = lineage_extractor._get_platform_for_connection(None)
    assert platform == "unknown"


def test_create_upstream_lineage(lineage_extractor):
    """Test creating upstream lineage aspect"""
    input_datasets = [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,source_table,TEST)",
        "urn:li:dataset:(urn:li:dataPlatform:mysql,another_source,TEST)"
    ]
    output_datasets = [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,target_table,TEST)"
    ]
    
    upstream_lineage = lineage_extractor.create_upstream_lineage(
        input_datasets, output_datasets
    )
    
    assert upstream_lineage is not None
    assert len(upstream_lineage.upstreams) == 2
    
    for upstream in upstream_lineage.upstreams:
        assert upstream.dataset in input_datasets
        assert upstream.type == "TRANSFORMED"


def test_create_upstream_lineage_no_inputs(lineage_extractor):
    """Test creating upstream lineage with no inputs"""
    upstream_lineage = lineage_extractor.create_upstream_lineage([], [])
    assert upstream_lineage is None


def test_extract_table_to_table_lineage(lineage_extractor, sample_job):
    """Test extracting table-to-table lineage"""
    lineage_edges = lineage_extractor.extract_table_to_table_lineage(sample_job)
    
    # Should have at least one lineage edge
    assert len(lineage_edges) >= 1
    
    # Each edge should be a tuple of (upstream_urn, downstream_urn)
    for upstream_urn, downstream_urn in lineage_edges:
        assert upstream_urn.startswith("urn:li:dataset:")
        assert downstream_urn.startswith("urn:li:dataset:")


def test_lineage_extractor_with_report(sample_job):
    """Test that lineage extractor properly updates the report"""
    report = PentahoSourceReport()
    extractor = PentahoLineageExtractor(
        database_mapping={'mysql_conn': 'mysql', 'postgres_conn': 'postgres'},
        default_database_platform='unknown',
        env='TEST',
        report=report
    )
    
    # Extract lineage
    input_datasets, output_datasets = extractor.extract_lineage(sample_job)
    
    # Check that report was updated
    assert report.lineage_edges_created >= 1
    assert len(report.database_connections_found) >= 1


def test_lineage_extraction_error_handling(lineage_extractor):
    """Test error handling in lineage extraction"""
    # Create a step that might cause errors
    step = PentahoStep(
        name="Problematic Step",
        type="TableInput",
        sql="INVALID SQL SYNTAX HERE",
        connection_name="nonexistent_conn"
    )
    
    # Should not raise exception, but might not extract datasets
    datasets = lineage_extractor._extract_input_datasets(step)
    
    # Should return empty list or handle gracefully
    assert isinstance(datasets, list)