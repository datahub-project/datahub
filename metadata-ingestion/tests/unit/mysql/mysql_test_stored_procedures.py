import re

from pydantic import SecretStr

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.sql.mysql import MySQLConfig
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
