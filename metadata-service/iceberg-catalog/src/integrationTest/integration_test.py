from pyspark.sql import SparkSession
import os
from urllib.parse import urlparse
import pytest
from datahub.cli import cli_utils, iceberg_cli
from datahub.ingestion.graph.client import get_default_graph


def get_gms_url():
    return os.getenv("DATAHUB_GMS_URL") or "http://localhost:8080"


@pytest.fixture
def personal_access_token():
    username = "datahub"
    password = "datahub"
    token_name, token = cli_utils.generate_access_token(
        username, password, get_gms_url()
    )

    # Setting this env var makes get_default_graph use these env vars to create a graphql client.
    os.environ["DATAHUB_GMS_TOKEN"] = token
    os.environ["DATAHUB_GMS_HOST"] = urlparse(get_gms_url()).hostname
    os.environ["DATAHUB_GMS_PORT"] = str(urlparse(get_gms_url()).port)

    yield token

    # revoke token


def give_all_permissions(username, policy_name):
    client = get_default_graph()
    query = """
        mutation createAdminRole($policyName: String!, $user: String!) {
          createPolicy(
            input: {
                name: $policyName, 
                description: "For Testing", 
                state: ACTIVE, 
                type: METADATA, 
                privileges: ["DATA_READ_WRITE", "DATA_MANAGE_NAMESPACES", "DATA_MANAGE_TABLES", "DATA_MANAGE_VIEWS", "DATA_MANAGE_NAMESPACES", "DATA_LIST_ENTITIES"], 
                actors: {users: [$user], 
                allUsers: false, 
                resourceOwners: true, 
                allGroups: false}}
          )
        }
        """
    variables = {"user": f"urn:li:corpuser:{username}", "policyName": policy_name}

    client.execute_graphql(query, variables=variables, format_exception=False)


@pytest.fixture
def spark_session(personal_access_token, warehouse):
    # Create a Spark session

    spark = (
        SparkSession.builder.appName("Simple Example")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1",
        )
        .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.test.type", "rest")
        .config("spark.sql.catalog.test.uri", f"{get_gms_url()}/iceberg")
        .config("spark.sql.catalog.test.warehouse", warehouse)
        .config("spark.sql.catalog.test.token", personal_access_token)
        .config("spark.sql.defaultCatalog", "test")
        .config("spark.sql.catalog.test.default-namespace", "default")
        .config(
            "spark.sql.catalog.test.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config("spark.sql.catalog.test.rest-metrics-reporting-enabled", False)
        .master("local[*]")
        .getOrCreate()
    )

    # ensure default namespace
    spark.sql("create namespace if not exists default")

    yield spark

    # Stop the Spark session
    spark.stop()


@pytest.fixture(params=[f"test_wh_{index}" for index in range(4)])
def warehouse(request, personal_access_token):
    warehouse_name = request.param
    # PAT dependency just to ensure env vars are setup with token
    give_all_permissions("datahub", "test-policy")

    data_root = os.getenv(
        "ICEBERG_DATA_ROOT", f"s3://srinath-dev/test/{warehouse_name}"
    )
    client_id = os.getenv("ICEBERG_CLIENT_ID")
    client_secret = os.getenv("ICEBERG_CLIENT_SECRET")
    region = os.getenv("ICEBERG_REGION")
    role = os.getenv("ICEBERG_ROLE")

    if not all((data_root, client_id, client_secret, region, role)):
        pytest.fail(
            "Must set ICEBERG_DATA_ROOT, ICEBERG_CLIENT_ID, ICEBERG_CLIENT_SECRET, ICEBERG_REGION, ICEBERG_ROLE"
        )

    try:
        iceberg_cli.delete.callback(warehouse_name, dry_run=False, force=True)
        print(
            f"Deleted warehouse {warehouse_name}"
        )  # This ensures we are starting with a new warehouse.
    except Exception as e:
        print(e)

    iceberg_cli.create.callback(
        warehouse=warehouse_name,
        description="",
        data_root=data_root,
        client_id=client_id,
        client_secret=client_secret,
        region=region,
        role=role,
        env="PROD",
        duration_seconds=60 * 60,
    )

    yield warehouse_name


def cleanup(session):
    # Cleanup any remnants of past test runs
    session.sql("drop table if exists test_table")
    session.sql("drop view if exists test_view")


def _test_basic_table_ops(spark_session):
    spark_session.sql("create table test_table (id int, name string)")

    spark_session.sql("insert into test_table values(1, 'foo' ) ")
    result = spark_session.sql("SELECT * FROM test_table")
    assert result.count() == 1

    spark_session.sql("update test_table set name='bar' where id=1")
    result = spark_session.sql("SELECT * FROM test_table where name='bar'")
    assert result.count() == 1

    spark_session.sql("delete from test_table")
    result = spark_session.sql("SELECT * FROM test_table")
    assert result.count() == 0

    spark_session.sql("drop table test_table")
    with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark_session.sql("select * from test_table")

    # TODO: Add dataset verification


def _test_basic_view_ops(spark_session):
    spark_session.sql("create table test_table (id int, name string)")
    spark_session.sql("insert into test_table values(1, 'foo' ) ")

    spark_session.sql("create view test_view AS select * from test_table")
    result = spark_session.sql("SELECT * FROM test_view")
    assert result.count() == 1

    spark_session.sql("DROP VIEW test_view")
    with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark_session.sql("SELECT * FROM test_view")

    spark_session.sql("drop table test_table")


def _test_rename_ops(spark_session):
    spark_session.sql("create table test_table (id int, name string)")
    spark_session.sql("insert into test_table values(1, 'foo' ) ")

    spark_session.sql("alter table test_table rename to test_table_renamed")

    with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark_session.sql("SELECT * FROM test_table")

    spark_session.sql("insert into test_table_renamed values(2, 'bar' ) ")
    result = spark_session.sql("SELECT * FROM test_table_renamed")
    assert result.count() == 2

    spark_session.sql("create view test_view as select * from test_table_renamed")
    result = spark_session.sql("SELECT * FROM test_view")
    assert result.count() == 2

    spark_session.sql("alter view test_view rename to test_view_renamed")
    result = spark_session.sql("SELECT * FROM test_view_renamed")
    assert result.count() == 2

    spark_session.sql("drop view test_view_renamed")
    spark_session.sql("drop view test_table_renamed")


@pytest.mark.quick
@pytest.mark.parametrize("warehouse", ["test_wh_0"], indirect=True)
def test_iceberg_quick(spark_session, warehouse):
    spark_session.sql("use namespace default")
    _test_basic_table_ops(spark_session)
    _test_basic_view_ops(spark_session)
    _test_rename_ops(spark_session)


def _create_table(spark_session, ns, table_name):
    spark_session.sql("create namespace if not exists default")
    spark_session.sql(f"create namespace if not exists {ns}")
    spark_session.sql(f"drop table if exists {ns}.{table_name}")
    spark_session.sql(f"create table {ns}.{table_name} (id int, name string)")

    spark_session.sql(f"insert into {ns}.{table_name} values (1, 'foo' ) ")


def test_load_tables(spark_session, warehouse):
    namespace_count = 3
    table_count = 4
    for ns_index in range(namespace_count):
        ns = f"default_ns{ns_index}"
        for table_index in range(table_count):
            table_name = f"table_{table_index}"
            _create_table(spark_session, ns, table_name)
            _create_table(spark_session, ns, table_name)
