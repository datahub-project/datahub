import os
from typing import Dict, Set

import setuptools

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
    "click>=7.1.1",
    "pyyaml>=5.4.1",
    "toml>=0.10.0",
    "docker>=4.4",
    "expandvars>=0.6.5",
    "avro-gen3==0.3.8",
    "avro-python3>=1.8.2",
}

kafka_common = {
    # We currently require both Avro libraries. The codegen uses avro-python3 (above)
    # schema parsers at runtime for generating and reading JSON into Python objects.
    # At the same time, we use Kafka's AvroSerializer, which internally relies on
    # fastavro for serialization. We do not use confluent_kafka[avro], since it
    # is incompatible with its own dep on avro-python3.
    "confluent_kafka>=1.5.0",
    "fastavro>=1.3.0",
}

sql_common = {
    # Required for all SQL sources.
    "sqlalchemy>=1.3.23",
}

# Note: for all of these, framework_common will be added.
plugins: Dict[str, Set[str]] = {
    # Source plugins
    "kafka": kafka_common,
    "athena": sql_common | {"PyAthena[SQLAlchemy]"},
    "bigquery": sql_common
    | {
        # This will change to a normal reference to pybigquery once a new version is released to PyPI.
        # We need to use this custom version in order to correctly get table descriptions.
        # See this PR by hsheth2 for details: https://github.com/tswast/pybigquery/pull/82.
        # "pybigquery @ git+https://github.com/tswast/pybigquery@3250fa796b28225cb1c89d7afea3c2e2a2bf2305#egg=pybigquery"
        "pybigquery"
    },
    "hive": sql_common | {"pyhive[hive]"},
    "mssql": sql_common | {"sqlalchemy-pytds>=0.3"},
    "mysql": sql_common | {"pymysql>=1.0.2"},
    "postgres": sql_common | {"psycopg2-binary", "GeoAlchemy2"},
    "snowflake": sql_common | {"snowflake-sqlalchemy"},
    "ldap": {"python-ldap>=2.4"},
    "druid": sql_common | {"pydruid>=0.6.2"},
    "mongodb": {"pymongo>=3.11"},
    # Sink plugins.
    "datahub-kafka": kafka_common,
    "datahub-rest": {"requests>=2.25.1"},
    # Integrations.
    "airflow": {"apache-airflow >= 1.10.3"},
}

dev_requirements = {
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
    "sqlalchemy-stubs",
    "deepdiff",
    "build",
    "twine",
    # Also add the plugins which are used for tests.
    *list(
        dependency
        for plugin in [
            "bigquery",
            "mysql",
            "mssql",
            "mongodb",
            "ldap",
            "datahub-kafka",
            "datahub-rest",
            "airflow",
        ]
        for dependency in plugins[plugin]
    ),
    "apache-airflow-providers-snowflake",  # Used in the example DAGs.
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
    include_package_data=True,
    package_data={
        "datahub": ["py.typed"],
        "datahub.metadata": ["schema.avsc"],
    },
    entry_points={
        "console_scripts": ["datahub = datahub.entrypoints:datahub"],
        "datahub.ingestion.source.plugins": [
            "file = datahub.ingestion.source.mce_file:MetadataFileSource",
            "athena = datahub.ingestion.source.athena:AthenaSource",
            "bigquery = datahub.ingestion.source.bigquery:BigQuerySource",
            "dbt = datahub.ingestion.source.dbt:DBTSource",
            "druid = datahub.ingestion.source.druid:DruidSource",
            "hive = datahub.ingestion.source.hive:HiveSource",
            "kafka = datahub.ingestion.source.kafka:KafkaSource",
            "ldap = datahub.ingestion.source.ldap:LDAPSource",
            "mongodb = datahub.ingestion.source.mongodb:MongoDBSource",
            "mssql = datahub.ingestion.source.mssql:SQLServerSource",
            "mysql = datahub.ingestion.source.mysql:MySQLSource",
            "postgres = datahub.ingestion.source.postgres:PostgresSource",
            "snowflake = datahub.ingestion.source.snowflake:SnowflakeSource",
        ],
        "datahub.ingestion.sink.plugins": [
            "file = datahub.ingestion.sink.file:FileSink",
            "console = datahub.ingestion.sink.console:ConsoleSink",
            "datahub-kafka = datahub.ingestion.sink.datahub_kafka:DatahubKafkaSink",
            "datahub-rest = datahub.ingestion.sink.datahub_rest:DatahubRestSink",
        ],
        "apache_airflow_provider": [
            "provider_info=datahub.integrations.airflow.get_provider_info:get_provider_info"
        ],
    },
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
    },
)
