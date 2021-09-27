import os
import sys
from typing import Dict, Set

import setuptools

is_py37_or_newer = sys.version_info >= (3, 7)


package_metadata: dict = {}
with open("./src/datahub/__init__.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md")) as f:
        description = f.read()

    return description


base_requirements = {
    # Compatability.
    "dataclasses>=0.6; python_version < '3.7'",
    "typing_extensions>=3.7.4; python_version < '3.8'",
    "mypy_extensions>=0.4.3",
    # Actual dependencies.
    "typing-inspect",
    "pydantic>=1.5.1",
}

framework_common = {
    "click>=6.0.0",
    "click-default-group",
    "PyYAML",
    "toml>=0.10.0",
    "entrypoints",
    "docker",
    "expandvars>=0.6.5",
    "avro-gen3==0.6.0",
    "avro-python3>=1.8.2",
    "python-dateutil>=2.8.0",
    "stackprinter",
    "tabulate",
}

kafka_common = {
    # We currently require both Avro libraries. The codegen uses avro-python3 (above)
    # schema parsers at runtime for generating and reading JSON into Python objects.
    # At the same time, we use Kafka's AvroSerializer, which internally relies on
    # fastavro for serialization. We do not use confluent_kafka[avro], since it
    # is incompatible with its own dep on avro-python3.
    "confluent_kafka>=1.5.0",
    "fastavro>=1.2.0",
}

sql_common = {
    # Required for all SQL sources.
    "sqlalchemy==1.3.24",
}

aws_common = {
    # AWS Python SDK
    "boto3"
}

looker_common = {
    # Looker Python SDK
    "looker-sdk==21.6.0"
}

# Note: for all of these, framework_common will be added.
plugins: Dict[str, Set[str]] = {
    # Sink plugins.
    "datahub-kafka": kafka_common,
    "datahub-rest": {"requests"},
    # Integrations.
    "airflow": {
        "apache-airflow >= 1.10.2",
    },
    # Source plugins
    "athena": sql_common | {"PyAthena[SQLAlchemy]"},
    "azure-ad": set(),
    "bigquery": sql_common | {"pybigquery >= 0.6.0"},
    "bigquery-usage": {"google-cloud-logging", "cachetools"},
    "datahub-business-glossary": set(),
    "dbt": set(),
    "druid": sql_common | {"pydruid>=0.6.2"},
    "feast": {"docker"},
    "glue": aws_common,
    "hive": sql_common
    | {
        # Acryl Data maintains a fork of PyHive, which adds support for table comments
        # and column comments, and also releases HTTP and HTTPS transport schemes.
        "acryl-pyhive[hive]>=0.6.11"
    },
    "kafka": kafka_common,
    "kafka-connect": sql_common | {"requests"},
    "ldap": {"python-ldap>=2.4"},
    "looker": looker_common,
    "lookml": looker_common | {"lkml>=1.1.0", "sql-metadata==2.2.1"},
    "mongodb": {"pymongo>=3.11"},
    "mssql": sql_common | {"sqlalchemy-pytds>=0.3"},
    "mssql-odbc": sql_common | {"pyodbc"},
    "mysql": sql_common | {"pymysql>=1.0.2"},
    "okta": {"okta~=1.7.0"},
    "oracle": sql_common | {"cx_Oracle"},
    "postgres": sql_common | {"psycopg2-binary", "GeoAlchemy2"},
    "redash": {"redash-toolbelt"},
    "redshift": sql_common | {"sqlalchemy-redshift", "psycopg2-binary", "GeoAlchemy2"},
    "redshift-usage": sql_common
    | {"sqlalchemy-redshift", "psycopg2-binary", "GeoAlchemy2"},
    "sagemaker": aws_common,
    "snowflake": sql_common | {"snowflake-sqlalchemy<=1.2.4"},
    "snowflake-usage": sql_common | {"snowflake-sqlalchemy<=1.2.4"},
    "sqlalchemy": sql_common,
    "sql-profiles": sql_common | {"great-expectations"},
    "superset": {"requests"},
}

all_exclude_plugins: Set[str] = {
    # SQL Server ODBC requires additional drivers, and so we don't want to keep
    # it included in the default "all" installation.
    "mssql-odbc",
}

mypy_stubs = {
    "types-dataclasses",
    "sqlalchemy-stubs",
    "types-pkg_resources",
    "types-six",
    "types-python-dateutil",
    "types-requests",
    "types-toml",
    "types-PyMySQL",
    "types-PyYAML",
    "types-freezegun",
    "types-cachetools",
    # versions 0.1.13 and 0.1.14 seem to have issues
    "types-click==0.1.12",
    "boto3-stubs[s3,glue,sagemaker]",
    "types-tabulate",
}

base_dev_requirements = {
    *base_requirements,
    *framework_common,
    *mypy_stubs,
    "black>=19.10b0",
    "coverage>=5.1",
    "flake8>=3.8.3",
    "flake8-tidy-imports>=4.3.0",
    "isort>=5.7.0",
    "mypy>=0.901",
    "pytest>=6.2.2",
    "pytest-cov>=2.8.1",
    "pytest-docker>=0.10.3",
    "tox",
    "deepdiff",
    "requests-mock",
    "freezegun",
    "jsonpickle",
    "build",
    "twine",
    *list(
        dependency
        for plugin in [
            "bigquery",
            "bigquery-usage",
            "looker",
            "glue",
            "okta",
            "oracle",
            "postgres",
            "sagemaker",
            "datahub-kafka",
            "datahub-rest",
            "redash",
            "redshift",
            "redshift-usage"
            # airflow is added below
        ]
        for dependency in plugins[plugin]
    ),
}

if is_py37_or_newer:
    # The lookml plugin only works on Python 3.7 or newer.
    base_dev_requirements = base_dev_requirements.union(
        {dependency for plugin in ["lookml"] for dependency in plugins[plugin]}
    )

dev_requirements = {
    *base_dev_requirements,
    "apache-airflow[snowflake]>=2.0.2",  # snowflake is used in example dags
    "snowflake-sqlalchemy<=1.2.4",  # make constraint consistent with extras
}
dev_requirements_airflow_1 = {
    *base_dev_requirements,
    "apache-airflow==1.10.15",
    "apache-airflow-backport-providers-snowflake",
    "snowflake-sqlalchemy<=1.2.4",  # make constraint consistent with extras
}

full_test_dev_requirements = {
    *list(
        dependency
        for plugin in [
            "druid",
            "feast",
            "hive",
            "ldap",
            "mongodb",
            "mssql",
            "mysql",
            "snowflake",
            "sql-profiles",
            "redash",
        ]
        for dependency in plugins[plugin]
    ),
}

entry_points = {
    "console_scripts": ["datahub = datahub.entrypoints:main"],
    "datahub.ingestion.source.plugins": [
        "file = datahub.ingestion.source.file:GenericFileSource",
        "sqlalchemy = datahub.ingestion.source.sql.sql_generic:SQLAlchemyGenericSource",
        "athena = datahub.ingestion.source.sql.athena:AthenaSource",
        "azure-ad = datahub.ingestion.source.identity.azure_ad:AzureADSource",
        "bigquery = datahub.ingestion.source.sql.bigquery:BigQuerySource",
        "bigquery-usage = datahub.ingestion.source.usage.bigquery_usage:BigQueryUsageSource",
        "dbt = datahub.ingestion.source.dbt:DBTSource",
        "druid = datahub.ingestion.source.sql.druid:DruidSource",
        "feast = datahub.ingestion.source.feast:FeastSource",
        "glue = datahub.ingestion.source.aws.glue:GlueSource",
        "sagemaker = datahub.ingestion.source.aws.sagemaker:SagemakerSource",
        "hive = datahub.ingestion.source.sql.hive:HiveSource",
        "kafka = datahub.ingestion.source.kafka:KafkaSource",
        "kafka-connect = datahub.ingestion.source.kafka_connect:KafkaConnectSource",
        "ldap = datahub.ingestion.source.ldap:LDAPSource",
        "looker = datahub.ingestion.source.looker:LookerDashboardSource",
        "lookml = datahub.ingestion.source.lookml:LookMLSource",
        "datahub-business-glossary = datahub.ingestion.source.metadata.business_glossary:BusinessGlossaryFileSource",
        "mongodb = datahub.ingestion.source.mongodb:MongoDBSource",
        "mssql = datahub.ingestion.source.sql.mssql:SQLServerSource",
        "mysql = datahub.ingestion.source.sql.mysql:MySQLSource",
        "okta = datahub.ingestion.source.identity.okta:OktaSource",
        "oracle = datahub.ingestion.source.sql.oracle:OracleSource",
        "postgres = datahub.ingestion.source.sql.postgres:PostgresSource",
        "redash = datahub.ingestion.source.redash:RedashSource",
        "redshift = datahub.ingestion.source.sql.redshift:RedshiftSource",
        "redshift-usage = datahub.ingestion.source.usage.redshift_usage:RedshiftUsageSource",
        "snowflake = datahub.ingestion.source.sql.snowflake:SnowflakeSource",
        "snowflake-usage = datahub.ingestion.source.usage.snowflake_usage:SnowflakeUsageSource",
        "superset = datahub.ingestion.source.superset:SupersetSource",
    ],
    "datahub.ingestion.sink.plugins": [
        "file = datahub.ingestion.sink.file:FileSink",
        "console = datahub.ingestion.sink.console:ConsoleSink",
        "datahub-kafka = datahub.ingestion.sink.datahub_kafka:DatahubKafkaSink",
        "datahub-rest = datahub.ingestion.sink.datahub_rest:DatahubRestSink",
    ],
    "apache_airflow_provider": ["provider_info=datahub_provider:get_provider_info"],
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://datahubproject.io/",
    project_urls={
        "Documentation": "https://datahubproject.io/docs/",
        "Source": "https://github.com/linkedin/datahub",
        "Changelog": "https://github.com/linkedin/datahub/releases",
    },
    license="Apache License 2.0",
    description="A CLI to work with DataHub metadata",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ],
    # Package info.
    zip_safe=False,
    python_requires=">=3.6",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    package_data={
        "datahub": ["py.typed"],
        "datahub.metadata": ["schema.avsc"],
        "datahub.metadata.schemas": ["*.avsc"],
        "datahub.ingestion.source.feast_image": ["Dockerfile", "requirements.txt"],
    },
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements | framework_common),
    extras_require={
        "base": list(framework_common),
        **{
            plugin: list(framework_common | dependencies)
            for (plugin, dependencies) in plugins.items()
        },
        "all": list(
            framework_common.union(
                *[
                    requirements
                    for plugin, requirements in plugins.items()
                    if plugin not in all_exclude_plugins
                ]
            )
        ),
        "dev": list(dev_requirements),
        "dev-airflow1": list(dev_requirements_airflow_1),
        "integration-tests": list(full_test_dev_requirements),
    },
)
