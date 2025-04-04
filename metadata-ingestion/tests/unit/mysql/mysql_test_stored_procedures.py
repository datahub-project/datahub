import re

from pydantic import SecretStr

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.mysql.job_models import (
    MySQLDataJob,
    MySQLProcedureContainer,
    MySQLStoredProcedure,
)


def test_stored_procedure_parsing():
    """Test parsing of a stored procedure definition"""
    procedure = MySQLStoredProcedure(
        routine_schema="test_db",
        routine_name="test_proc",
        flow=MySQLProcedureContainer(
            name="test_db.stored_procedures",
            env="PROD",
            db="test_db",
            platform_instance=None,
        ),
        code="""
        CREATE PROCEDURE test_proc()
        BEGIN
            INSERT INTO target_table
            SELECT * FROM source_table;
        END
        """,
    )

    assert procedure.routine_name == "test_proc"
    assert procedure.routine_schema == "test_db"
    assert procedure.full_name == "test_db.test_proc"
    assert procedure.full_type == "MYSQL_STORED_PROCEDURE"


def test_procedure_container_properties():
    """Test MySQL procedure container property handling"""
    container = MySQLProcedureContainer(
        name="test_db.stored_procedures",
        env="PROD",
        db="test_db",
        platform_instance="local",
    )

    assert container.formatted_name == "test_db.stored_procedures"
    assert container.orchestrator == "mysql"
    assert container.cluster == "PROD"
    assert container.full_type == "(mysql,test_db.stored_procedures,PROD)"


def test_stored_procedure_config():
    """Test stored procedure configuration options"""
    config = MySQLConfig(
        host_port="localhost:3306",
        username="user",
        password=SecretStr("pass"),
        database="test_db",
        include_stored_procedures=True,
        include_stored_procedures_code=True,
        include_lineage=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"], deny=[".*_temp"]),
    )

    assert config.include_stored_procedures
    assert config.include_stored_procedures_code
    assert config.include_lineage
    assert config.procedure_pattern.allowed("test_db.my_proc")
    assert not config.procedure_pattern.allowed("test_db.my_proc_temp")
    assert not config.procedure_pattern.allowed("other_db.proc")


def test_temp_table_identification():
    """Test temporary table identification logic"""

    def is_temp_table(name: str) -> bool:
        # Use regex patterns to ensure exact matches
        patterns = [
            r"^temp_",  # Starts with temp_
            r"^tmp_",  # Starts with tmp_
            r"_temp$",  # Ends with _temp
            r"_tmp$",  # Ends with _tmp
            r"_temp_",  # Has _temp_ in middle
            r"_tmp_",  # Has _tmp_ in middle
        ]
        return any(re.search(pattern, name.lower()) for pattern in patterns)

    # Should match temporary table patterns
    assert is_temp_table("temp_customers")
    assert is_temp_table("TMP_DATA")
    assert is_temp_table("my_temp_table")
    assert is_temp_table("my_tmp_table")

    # Should not match non-temporary tables
    assert not is_temp_table("customers")
    assert not is_temp_table("template_table")
    assert not is_temp_table("temperature")
    assert not is_temp_table("temporary")


def test_procedure_metadata_handling():
    """Test handling of stored procedure metadata"""
    procedure = MySQLStoredProcedure(
        routine_schema="test_db",
        routine_name="test_proc",
        flow=MySQLProcedureContainer(
            name="test_db.stored_procedures",
            env="PROD",
            db="test_db",
            platform_instance=None,
        ),
        code="CREATE PROCEDURE test_proc() BEGIN /* Test proc */ SELECT 1; END",
    )

    data_job = MySQLDataJob(entity=procedure)
    assert data_job.entity.full_name == "test_db.test_proc"

    # Test adding properties
    data_job.add_property("description", "Test procedure")
    data_job.add_property("created_by", "test_user")

    assert data_job.valued_properties == {
        "description": "Test procedure",
        "created_by": "test_user",
    }


def test_mysql_data_job_empty_properties():
    """Test MySQL data job with empty properties"""
    procedure = MySQLStoredProcedure(
        routine_schema="test_db",
        routine_name="test_proc",
        flow=MySQLProcedureContainer(
            name="test_db.stored_procedures",
            env="PROD",
            db="test_db",
            platform_instance=None,
        ),
    )

    data_job = MySQLDataJob(entity=procedure)

    # Test initial empty properties
    assert data_job.valued_properties == {}
    assert len(data_job.job_properties) == 0

    # Test empty string property - should be included since it's not None
    data_job.add_property("test", "")
    assert "test" in data_job.valued_properties
    assert data_job.valued_properties["test"] == ""

    # Test string "None" property - should be included since it's not None
    data_job.add_property("test2", "None")
    assert "test2" in data_job.valued_properties
    assert data_job.valued_properties["test2"] == "None"


def test_mysql_procedure_platform_instance():
    """Test MySQL procedure with platform instance"""
    container = MySQLProcedureContainer(
        name="test_db.stored_procedures",
        env="PROD",
        db="test_db",
        platform_instance="custom-instance",
    )

    procedure = MySQLStoredProcedure(
        routine_schema="test_db", routine_name="test_proc", flow=container
    )

    data_job = MySQLDataJob(entity=procedure)
    platform_instance = data_job.as_maybe_platform_instance_aspect

    assert platform_instance is not None
    # Check both platform and instance URNs
    assert platform_instance.platform == "urn:li:dataPlatform:mysql"
    assert (
        platform_instance.instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,custom-instance)"
    )


def test_mysql_data_job_aspects():
    """Test MySQL data job input/output aspects"""
    procedure = MySQLStoredProcedure(
        routine_schema="test_db",
        routine_name="test_proc",
        flow=MySQLProcedureContainer(
            name="test_db.stored_procedures",
            env="PROD",
            db="test_db",
            platform_instance=None,
        ),
    )

    data_job = MySQLDataJob(entity=procedure)

    # Add some test data
    data_job.incoming = ["dataset1", "dataset2"]
    data_job.outgoing = ["dataset3"]
    data_job.input_jobs = ["job1"]

    io_aspect = data_job.as_datajob_input_output_aspect
    assert sorted(io_aspect.inputDatasets) == ["dataset1", "dataset2"]
    assert io_aspect.outputDatasets == ["dataset3"]
    assert io_aspect.inputDatajobs == ["job1"]


def test_mysql_flow_container_formatting():
    """Test MySQL flow container name formatting"""
    container = MySQLProcedureContainer(
        name="test,db.stored,procedures",  # Contains commas
        env="PROD",
        db="test_db",
        platform_instance=None,
    )

    assert container.formatted_name == "test-db.stored-procedures"


def test_stored_procedure_properties():
    """Test stored procedure additional properties"""
    procedure = MySQLStoredProcedure(
        routine_schema="test_db",
        routine_name="test_proc",
        flow=MySQLProcedureContainer(
            name="test_db.stored_procedures",
            env="PROD",
            db="test_db",
            platform_instance=None,
        ),
        code="CREATE PROCEDURE test_proc() BEGIN SELECT 1; END",
    )

    data_job = MySQLDataJob(entity=procedure)

    # Test adding various properties
    data_job.add_property("CREATED", "2024-01-01")
    data_job.add_property("LAST_ALTERED", "2024-01-02")
    data_job.add_property("SQL_DATA_ACCESS", "MODIFIES")
    data_job.add_property("SECURITY_TYPE", "DEFINER")
    data_job.add_property("parameters", "IN param1 INT, OUT param2 VARCHAR")

    assert len(data_job.valued_properties) == 5
    assert all(value is not None for value in data_job.valued_properties.values())


def test_temp_table_patterns():
    """Test comprehensive temp table pattern matching"""
    config = MySQLConfig(
        schema_pattern=AllowDenyPattern(allow=["test_schema"]),
        table_pattern=AllowDenyPattern(allow=["test_schema.*"]),
    )
    source = MySQLSource(ctx=PipelineContext(run_id="test"), config=config)

    # Mock the discovered_datasets property to include all our "permanent" tables
    source.discovered_datasets = {
        "test_schema.permanent_table",
        "test_schema.regular_table",
        "test_schema.table",
    }

    test_cases = [
        ("test_schema.#temp", True),  # Starts with #
        ("test_schema._tmp", True),  # Starts with _tmp
        ("test_schema.regular_table", False),  # In discovered_datasets
        ("test_schema.table", False),  # In discovered_datasets
        ("test_schema.temp_123", True),  # Not in discovered_datasets
        ("other_schema.temp", False),  # Schema not allowed
    ]

    for table_name, expected in test_cases:
        actual = source.is_temp_table(table_name)
        assert actual == expected, (
            f"Failed for {table_name}. Expected {expected}, got {actual}"
        )
