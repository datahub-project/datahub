import pytest

from datahub.emitter.mce_builder import datahub_guid
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.source.common.subtypes import JobContainerSubTypes
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
    _get_procedure_flow_name,
    generate_procedure_lineage,
)
from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver


def test_oracle_function_with_while_loop_and_select_into():
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")
    view_urn = DatasetUrn("oracle", "TESTDB.ORDER_LINES").urn()
    schema_resolver.add_schema_metadata(
        urn=view_urn,
        schema_metadata=SchemaMetadataClass(
            schemaName="ORDER_LINES",
            platform="urn:li:dataPlatform:oracle",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="order_id",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="NUMBER",
                ),
                SchemaFieldClass(
                    fieldPath="line_num",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="NUMBER",
                ),
                SchemaFieldClass(
                    fieldPath="item_code",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="VARCHAR2",
                ),
                SchemaFieldClass(
                    fieldPath="tax_rate",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="NUMBER",
                ),
            ],
        ),
    )

    code = """
BEGIN
v_found:='N';

SELECT min(line_num) INTO v_line_num FROM order_lines WHERE order_id=order_id_in
AND item_code=item_code_in;

WHILE v_found='N' LOOP
  SELECT tax_rate
  INTO v_tax 
  FROM order_lines 
  WHERE order_id=order_id_in AND line_num=v_line_num;

  IF v_tax is null THEN
     v_line_num:=v_line_num -1;
  ELSE 
     v_found:='Y';
  END IF;

END LOOP;

tax_out:=v_tax;
RETURN tax_out;
END;
"""

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TESTDB",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
        raise_=False,
        procedure_name="get_tax_rate",
    )

    assert result is not None
    assert len(result.inputDatasets) == 1
    assert result.inputDatasets[0] == view_urn
    assert result.fineGrainedLineages is not None
    assert len(result.fineGrainedLineages) >= 2


@pytest.mark.parametrize(
    "source_table,target_table,source_schema,target_schema,code,procedure_name",
    [
        (
            "SOURCE_TABLE",
            "TARGET_TABLE",
            {"id": "NUMBER", "name": "VARCHAR2", "value": "NUMBER"},
            {"id": "NUMBER", "name": "VARCHAR2", "value": "NUMBER"},
            """
BEGIN
  INSERT INTO target_table (id, name, value)
  SELECT id, name, value
  FROM source_table
  WHERE active = 1;

  COMMIT;
END;
""",
            "simple_insert_select",
        ),
        (
            "SOURCE_TABLE",
            "TARGET_TABLE",
            {"id": "NUMBER", "name": "VARCHAR2"},
            {"id": "NUMBER", "name": "VARCHAR2"},
            """
BEGIN
  INSERT INTO target_table (id, name)
  SELECT id, name FROM source_table;

  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error occurred');
      ROLLBACK;
END;
""",
            "with_exception_handling",
        ),
        (
            "EMPLOYEES",
            "HIGH_EARNERS",
            {"emp_id": "NUMBER", "emp_name": "VARCHAR2", "salary": "NUMBER"},
            {"emp_id": "NUMBER", "emp_name": "VARCHAR2"},
            """
DECLARE
  v_threshold NUMBER := 50000;
BEGIN
  FOR i IN 1..10 LOOP
    INSERT INTO high_earners (emp_id, emp_name)
    SELECT emp_id, emp_name 
    FROM employees 
    WHERE salary > v_threshold;
  END LOOP;
END;
""",
            "with_for_loop",
        ),
    ],
    ids=["simple_insert", "exception_handling", "for_loop"],
)
def test_oracle_insert_select_patterns(
    source_table, target_table, source_schema, target_schema, code, procedure_name
):
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")
    source_urn = DatasetUrn("oracle", f"TESTDB.{source_table}").urn()
    schema_resolver.add_raw_schema_info(urn=source_urn, schema_info=source_schema)
    target_urn = DatasetUrn("oracle", f"TESTDB.{target_table}").urn()
    schema_resolver.add_raw_schema_info(urn=target_urn, schema_info=target_schema)

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TESTDB",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
        raise_=False,
        procedure_name=procedure_name,
    )

    assert result is not None
    assert source_urn in result.inputDatasets
    assert target_urn in result.outputDatasets


def test_oracle_filter_variable_declarations():
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")

    code = """
DECLARE
  v_count NUMBER;
  v_name VARCHAR2(100);
BEGIN
  v_count := 0;
  v_name := 'test';
  
  RETURN v_count;
END;
"""

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TESTDB",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
        raise_=False,
        procedure_name="test_proc",
    )

    assert result is None or (not result.inputDatasets and not result.outputDatasets)


def test_oracle_procedure_to_procedure_lineage():
    """Test that Oracle procedure dependencies are converted to DataJob lineage edges."""
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")

    source_urn = DatasetUrn("oracle", "testdb.testschema.source_table").urn()
    schema_resolver.add_raw_schema_info(
        urn=source_urn,
        schema_info={"id": "NUMBER", "value": "NUMBER"},
    )

    target_urn = DatasetUrn("oracle", "testdb.testschema.target_table").urn()
    schema_resolver.add_raw_schema_info(
        urn=target_urn,
        schema_info={"id": "NUMBER", "value": "NUMBER"},
    )

    procedure = BaseProcedure(
        name="main_procedure",
        language="SQL",
        argument_signature=None,
        return_type=None,
        procedure_definition="""
BEGIN
  INSERT INTO target_table (id, value)
  SELECT id, value FROM source_table;
END;
""",
        created=None,
        last_altered=None,
        comment=None,
        extra_properties={
            "upstream_dependencies": "TESTDB.HELPER_PROC (PROCEDURE), TESTDB.CALC_FUNC (FUNCTION)"
        },
    )

    database_key = DatabaseKey(
        database="testdb",
        platform="oracle",
        instance=None,
        env="PROD",
        backcompat_env_as_instance=True,
    )

    schema_key = SchemaKey(
        database="testdb",
        schema="testschema",
        platform="oracle",
        instance=None,
        env="PROD",
        backcompat_env_as_instance=True,
    )

    job_urn = procedure.to_urn(database_key, schema_key)

    lineage_mcps = list(
        generate_procedure_lineage(
            schema_resolver=schema_resolver,
            procedure=procedure,
            procedure_job_urn=job_urn,
            default_db="testdb",
            default_schema="testschema",
            database_key=database_key,
            schema_key=schema_key,
        )
    )

    assert len(lineage_mcps) == 1

    datajob_input_output = lineage_mcps[0].aspect
    assert isinstance(datajob_input_output, DataJobInputOutputClass)

    assert source_urn in datajob_input_output.inputDatasets

    assert datajob_input_output.inputDatajobs is not None
    assert len(datajob_input_output.inputDatajobs) == 2

    input_job_urns = datajob_input_output.inputDatajobs
    assert any("helper_proc" in urn.lower() for urn in input_job_urns)
    assert any("calc_func" in urn.lower() for urn in input_job_urns)


def test_oracle_procedure_to_procedure_lineage_with_overloaded_procedures():
    """Test that overloaded procedures (with argument signatures) generate correct URNs with hash suffixes."""
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")

    source_urn = DatasetUrn("oracle", "testdb.testschema.source_table").urn()
    schema_resolver.add_raw_schema_info(
        urn=source_urn,
        schema_info={"id": "NUMBER", "value": "NUMBER"},
    )

    target_urn = DatasetUrn("oracle", "testdb.testschema.target_table").urn()
    schema_resolver.add_raw_schema_info(
        urn=target_urn,
        schema_info={"id": "NUMBER", "value": "NUMBER"},
    )

    # Create procedure registry with an overloaded procedure that has a signature hash
    calc_signature = "v_amount IN NUMBER, v_rate IN NUMBER"
    calc_hash = datahub_guid(dict(argument_signature=calc_signature))
    procedure_registry = {
        "testdb.helper_proc": "helper_proc",  # No signature
        "testdb.calc_func": f"calc_func_{calc_hash}",  # Has signature hash
    }

    procedure = BaseProcedure(
        name="main_procedure",
        language="SQL",
        argument_signature=None,
        return_type=None,
        procedure_definition="""
BEGIN
  INSERT INTO target_table (id, value)
  SELECT id, value FROM source_table;
END;
""",
        created=None,
        last_altered=None,
        comment=None,
        extra_properties={
            "upstream_dependencies": "TESTDB.HELPER_PROC (PROCEDURE), TESTDB.CALC_FUNC (FUNCTION)"
        },
    )

    database_key = DatabaseKey(
        database="testdb",
        platform="oracle",
        instance=None,
        env="PROD",
        backcompat_env_as_instance=True,
    )

    schema_key = SchemaKey(
        database="testdb",
        schema="testschema",
        platform="oracle",
        instance=None,
        env="PROD",
        backcompat_env_as_instance=True,
    )

    job_urn = procedure.to_urn(database_key, schema_key)

    lineage_mcps = list(
        generate_procedure_lineage(
            schema_resolver=schema_resolver,
            procedure=procedure,
            procedure_job_urn=job_urn,
            default_db="testdb",
            default_schema="testschema",
            database_key=database_key,
            schema_key=schema_key,
            procedure_registry=procedure_registry,
        )
    )

    assert len(lineage_mcps) == 1

    datajob_input_output = lineage_mcps[0].aspect
    assert isinstance(datajob_input_output, DataJobInputOutputClass)

    assert source_urn in datajob_input_output.inputDatasets

    assert datajob_input_output.inputDatajobs is not None
    assert len(datajob_input_output.inputDatajobs) == 2

    input_job_urns = datajob_input_output.inputDatajobs

    # Verify helper_proc (no signature) doesn't have hash
    helper_urn = [u for u in input_job_urns if "helper_proc" in u.lower()][0]
    assert helper_urn.endswith("helper_proc)")

    # Verify calc_func (with signature) has the correct hash
    calc_urn = [u for u in input_job_urns if "calc_func" in u.lower()][0]
    assert calc_hash in calc_urn

    # Verify calc_func (with signature) has the correct hash
    calc_urn = [u for u in input_job_urns if "calc_func" in u.lower()][0]
    assert calc_hash in calc_urn


def test_oracle_dataset_urn_format_without_database_name():
    """
    Test that dataset URNs in lineage don't include database name when default_db is None.
    This validates the fix for URN mismatch between tables and stored procedure lineage.
    """
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")

    employees_urn = DatasetUrn("oracle", "hr.employees").urn()
    schema_resolver.add_schema_metadata(
        urn=employees_urn,
        schema_metadata=SchemaMetadataClass(
            schemaName="employees",
            platform="urn:li:dataPlatform:oracle",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="employee_id",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="NUMBER",
                ),
                SchemaFieldClass(
                    fieldPath="department_id",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="NUMBER",
                ),
            ],
        ),
    )

    code = """
BEGIN
  INSERT INTO temp_employees (employee_id, department_id)
  SELECT employee_id, department_id
  FROM employees
  WHERE department_id = 100;
END;
"""

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db=None,
        default_schema="hr",
        code=code,
        is_temp_table=lambda t: t.startswith("temp_"),
        raise_=False,
        procedure_name="load_employees",
    )

    assert result is not None
    assert len(result.inputDatasets) == 1

    input_dataset_urn = result.inputDatasets[0]

    assert "hr.employees" in input_dataset_urn.lower()
    assert "orcl" not in input_dataset_urn.lower()
    assert input_dataset_urn == employees_urn


def test_oracle_dataset_urn_format_with_database_name():
    """
    Test that dataset URNs in lineage DO include database name when default_db is set.
    This validates the behavior when add_database_name_to_urn=True.
    """
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")

    employees_urn = DatasetUrn("oracle", "orcl.hr.employees").urn()
    schema_resolver.add_schema_metadata(
        urn=employees_urn,
        schema_metadata=SchemaMetadataClass(
            schemaName="employees",
            platform="urn:li:dataPlatform:oracle",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="employee_id",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="NUMBER",
                ),
                SchemaFieldClass(
                    fieldPath="department_id",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="NUMBER",
                ),
            ],
        ),
    )

    code = """
BEGIN
  INSERT INTO temp_employees (employee_id, department_id)
  SELECT employee_id, department_id
  FROM employees
  WHERE department_id = 100;
END;
"""

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="orcl",
        default_schema="hr",
        code=code,
        is_temp_table=lambda t: t.startswith("temp_"),
        raise_=False,
        procedure_name="load_employees",
    )

    assert result is not None
    assert len(result.inputDatasets) == 1

    input_dataset_urn = result.inputDatasets[0]

    assert "orcl.hr.employees" in input_dataset_urn.lower()
    assert input_dataset_urn == employees_urn


def test_oracle_procedure_flow_name_without_database():
    """
    Test that procedure flow names are correct for two-tier sources without database names.
    Functions and procedures should have separate containers.
    """
    database_key = DatabaseKey(
        database="",  # Empty database for two-tier sources
        platform="oracle",
        instance=None,
        env="PROD",
    )

    schema_key = SchemaKey(
        database="",  # Empty database
        schema="hr",
        platform="oracle",
        instance=None,
        env="PROD",
    )

    # Test procedure flow name
    proc_flow_name = _get_procedure_flow_name(
        database_key, schema_key, JobContainerSubTypes.STORED_PROCEDURE
    )
    assert proc_flow_name == "hr.stored_procedures"
    assert not proc_flow_name.startswith(".")

    # Test function flow name
    func_flow_name = _get_procedure_flow_name(
        database_key, schema_key, JobContainerSubTypes.FUNCTION
    )
    assert func_flow_name == "hr.functions"
    assert not func_flow_name.startswith(".")


def test_oracle_procedure_flow_name_with_database():
    """
    Test that procedure flow names include database when configured.
    Functions and procedures should have separate containers.
    """
    database_key = DatabaseKey(
        database="orcl",
        platform="oracle",
        instance=None,
        env="PROD",
    )

    schema_key = SchemaKey(
        database="orcl",
        schema="hr",
        platform="oracle",
        instance=None,
        env="PROD",
    )

    # Test procedure flow name
    proc_flow_name = _get_procedure_flow_name(
        database_key, schema_key, JobContainerSubTypes.STORED_PROCEDURE
    )
    assert proc_flow_name == "orcl.hr.stored_procedures"

    # Test function flow name
    func_flow_name = _get_procedure_flow_name(
        database_key, schema_key, JobContainerSubTypes.FUNCTION
    )
    assert func_flow_name == "orcl.hr.functions"


def test_oracle_function_subtype():
    """
    Test that Oracle functions get the FUNCTION subtype instead of STORED_PROCEDURE.
    """
    # Create a function
    oracle_function = BaseProcedure(
        name="get_employee_salary",
        language="SQL",
        argument_signature="employee_id NUMBER",
        return_type="NUMBER",
        procedure_definition="RETURN (SELECT salary FROM employees WHERE id = employee_id);",
        created=None,
        last_altered=None,
        comment="Returns employee salary",
        extra_properties={"object_type": "FUNCTION", "status": "VALID"},
        subtype=JobContainerSubTypes.FUNCTION,
    )

    # Create a procedure
    oracle_procedure = BaseProcedure(
        name="update_employee_salary",
        language="SQL",
        argument_signature="employee_id NUMBER, new_salary NUMBER",
        return_type=None,
        procedure_definition="UPDATE employees SET salary = new_salary WHERE id = employee_id;",
        created=None,
        last_altered=None,
        comment="Updates employee salary",
        extra_properties={"object_type": "PROCEDURE", "status": "VALID"},
        subtype=JobContainerSubTypes.STORED_PROCEDURE,
    )

    # Verify subtypes are correct
    assert oracle_function.subtype == JobContainerSubTypes.FUNCTION
    assert oracle_procedure.subtype == JobContainerSubTypes.STORED_PROCEDURE
    assert oracle_function.subtype != oracle_procedure.subtype
