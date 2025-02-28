import pandas as pd
from dagster import MaterializeResult, MetadataValue, asset
from dagster_aws.redshift import RedshiftClientResource
from dagster_snowflake import SnowflakeResource


@asset(
    metadata={"schema": "public"},
    key_prefix=["prod", "snowflake", "test_db", "public"],
    group_name="iris",
    io_manager_key="snowflake_io_manager",
)
def iris_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )


@asset(
    metadata={"schema": "public"},
    key_prefix=["prod", "snowflake", "test_db", "public"],
    group_name="iris",
    io_manager_key="snowflake_io_manager",
)
def iris_cleaned(iris_dataset: pd.DataFrame) -> pd.DataFrame:
    return iris_dataset.dropna().drop_duplicates()


@asset(
    metadata={"schema": "public"},
    key_prefix=["prod", "snowflake", "test_db", "public"],
    group_name="iris",
    deps=[iris_dataset],
)
def iris_setosa(snowflake: SnowflakeResource) -> MaterializeResult:
    query = """
        create or replace table TEST_DB.public.iris_setosa as (
            SELECT *
            FROM TEST_DB.public.iris_cleaned
            WHERE species = 'Iris-setosa'
        );
    """

    with snowflake.get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

    return MaterializeResult(
        metadata={
            "Query": MetadataValue.text(query),
        }
    )


@asset(
    key_prefix=[
        "prod",
        "snowflake",
        "db_name",
        "schema_name",
    ],  # the fqdn asset name to be able identify platform and make sure asset is unique
    group_name="iris",
    deps=[iris_dataset],
)
def my_asset_table_a(snowflake: SnowflakeResource) -> MaterializeResult:
    query = """
        create or replace table db_name.schema_name.my_asset_table_a as (
            SELECT *
            FROM db_name.schema_name.my_asset_table_b
        );
    """

    with snowflake.get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

    return MaterializeResult(  # Adding query to metadata to use it getting lineage from it with sql parser
        metadata={
            "Query": MetadataValue.text(query),
        }
    )


@asset(
    key_prefix=[
        "prod",
        "redshift",
        "dev",
        "public",
        "blood_storage_count",
    ],  # the fqdn asset name to be able identify platform and make sure asset is unique
    group_name="blood_storage",
)
def blood_storage_cleaned(redshift: RedshiftClientResource) -> MaterializeResult:
    query = """
        select count(*) from public.blood_storage;
    """
    client = redshift.get_client()
    client.execute_query(query)

    return MaterializeResult(  # Adding query to metadata to use it getting lineage from it with sql parser
        metadata={
            "Query": MetadataValue.text(query),
        }
    )
