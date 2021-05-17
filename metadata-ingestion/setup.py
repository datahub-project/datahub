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
    "PyYAML",
    "toml>=0.10.0",
    "entrypoints",
    "docker",
    "expandvars>=0.6.5",
    "avro-gen3==0.5.0",
    "avro-python3>=1.8.2",
    "python-dateutil",
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
    "sqlalchemy>=1.3.24",
}

# Note: for all of these, framework_common will be added.
plugins: Dict[str, Set[str]] = {
    # Sink plugins.
    "datahub-kafka": kafka_common,
    "datahub-rest": {"requests"},
    # Integrations.
    "airflow": {"apache-airflow >= 1.10.2"},
    # Source plugins
    "kafka": kafka_common,
    "sqlalchemy": sql_common,
    "athena": sql_common | {"PyAthena[SQLAlchemy]"},
    "bigquery": sql_common | {"pybigquery >= 0.6.0"},
    "hive": sql_common
    | {
        # Acryl Data maintains a fork of PyHive, which adds support for table comments
        # and column comments, and also releases HTTP and HTTPS transport schemes.
        "acryl-pyhive[hive]>=0.6.6"
    },
    "mssql": sql_common | {"sqlalchemy-pytds>=0.3"},
    "mysql": sql_common | {"pymysql>=1.0.2"},
    "postgres": sql_common | {"psycopg2-binary", "GeoAlchemy2"},
    "redshift": sql_common | {"psycopg2-binary", "GeoAlchemy2"},
    "snowflake": sql_common | {"snowflake-sqlalchemy"},
    "oracle": sql_common | {"cx_Oracle"},
    "ldap": {"python-ldap>=2.4"},
    "looker": {"looker-sdk==21.6.0"},
    "druid": sql_common | {"pydruid>=0.6.2"},
    "mongodb": {"pymongo>=3.11"},
    "superset": {"requests"},
    "glue": {"boto3"},
}
if is_py37_or_newer:
    plugins["lookml"] = {"lkml>=1.1.0", "sql-metadata==1.12.0"}


base_dev_requirements = {
    *base_requirements,
    *framework_common,
    "black>=19.10b0",
    "coverage>=5.1",
    "flake8>=3.8.3",
    "isort>=5.7.0",
    "mypy>=0.782",
    "pytest>=6.2.2",
    "pytest-cov>=2.8.1",
    "pytest-docker",
    "tox",
    "sqlalchemy-stubs",
    "deepdiff",
    "requests-mock",
    "freezegun",
    "build",
    "twine",
    *list(
        dependency
        for plugin in [
            "bigquery",
            "mysql",
            "mssql",
            "mongodb",
            "ldap",
            "looker",
            "glue",
            "datahub-kafka",
            "datahub-rest",
            # airflow is added below
        ]
        for dependency in plugins[plugin]
    ),
}

if is_py37_or_newer:
    base_dev_requirements = base_dev_requirements.union(
        {dependency for plugin in ["lookml"] for dependency in plugins[plugin]}
    )

dev_requirements = {
    *base_dev_requirements,
    "apache-airflow==1.10.15",
    "apache-airflow-backport-providers-snowflake",  # Used in the example DAGs.
}
dev_requirements_airflow_2 = {
    *base_dev_requirements,
    "apache-airflow>=2.0.2",
    "apache-airflow-providers-snowflake",
}


entry_points = {
    "console_scripts": ["datahub = datahub.entrypoints:main"],
    "datahub.ingestion.source.plugins": [
        "file = datahub.ingestion.source.mce_file:MetadataFileSource",
        "sqlalchemy = datahub.ingestion.source.sql_generic:SQLAlchemyGenericSource",
        "athena = datahub.ingestion.source.athena:AthenaSource",
        "bigquery = datahub.ingestion.source.bigquery:BigQuerySource",
        "dbt = datahub.ingestion.source.dbt:DBTSource",
        "druid = datahub.ingestion.source.druid:DruidSource",
        "glue = datahub.ingestion.source.glue:GlueSource",
        "hive = datahub.ingestion.source.hive:HiveSource",
        "kafka = datahub.ingestion.source.kafka:KafkaSource",
        "ldap = datahub.ingestion.source.ldap:LDAPSource",
        "looker = datahub.ingestion.source.looker:LookerDashboardSource",
        "mongodb = datahub.ingestion.source.mongodb:MongoDBSource",
        "mssql = datahub.ingestion.source.mssql:SQLServerSource",
        "mysql = datahub.ingestion.source.mysql:MySQLSource",
        "oracle = datahub.ingestion.source.oracle:OracleSource",
        "postgres = datahub.ingestion.source.postgres:PostgresSource",
        "redshift = datahub.ingestion.source.redshift:RedshiftSource",
        "snowflake = datahub.ingestion.source.snowflake:SnowflakeSource",
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

if is_py37_or_newer:
    entry_points["datahub.ingestion.source.plugins"].append(
        "lookml = datahub.ingestion.source.lookml:LookMLSource"
    )


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
    author="DataHub Committers",
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
        "all": list(framework_common.union(*plugins.values())),
        "dev": list(dev_requirements),
        "dev-airflow2": list(dev_requirements_airflow_2),
    },
)
