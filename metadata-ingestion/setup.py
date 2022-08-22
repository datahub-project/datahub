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
    # Typing extension should be >=3.10.0.2 ideally but we can't restrict due to Airflow 2.0.2 dependency conflict
    "typing_extensions>=3.7.4.3 ;  python_version < '3.8'",
    "typing_extensions>=3.10.0.2 ;  python_version >= '3.8'",
    "mypy_extensions>=0.4.3",
    # Actual dependencies.
    "typing-inspect",
    "pydantic>=1.5.1",
    "mixpanel>=4.9.0",
}

framework_common = {
    "click>=6.0.0",
    "click-default-group",
    "PyYAML",
    "toml>=0.10.0",
    "entrypoints",
    "docker",
    "expandvars>=0.6.5",
    "avro-gen3==0.7.4",
    "avro>=1.10.2,<1.11",
    "python-dateutil>=2.8.0",
    "stackprinter>=0.2.6",
    "tabulate",
    "progressbar2",
    "termcolor>=1.0.0",
    "types-termcolor>=1.0.0",
    "psutil>=5.8.0",
    "ratelimiter",
    # Markupsafe breaking change broke Jinja and some other libs
    # Pinning it to a version which works even though we are not using explicitly
    # https://github.com/aws/aws-sam-cli/issues/3661
    # Airflow compatibility: https://github.com/apache/airflow/blob/2.2.2/setup.cfg#L125
    "markupsafe>=1.1.1,<=2.0.1",
    "Deprecated",
    "types-Deprecated",
    "humanfriendly",
    "packaging",
    "aiohttp<4",
    "cached_property",
    "ijson",
    "tdigest"
}

kafka_common = {
    # The confluent_kafka package provides a number of pre-built wheels for
    # various platforms and architectures. However, it does not provide wheels
    # for arm64 (including M1 Macs) or aarch64 (Docker's linux/arm64). This has
    # remained an open issue on the confluent_kafka project for a year:
    #   - https://github.com/confluentinc/confluent-kafka-python/issues/1182
    #   - https://github.com/confluentinc/confluent-kafka-python/pull/1161
    #
    # When a wheel is not available, we must build from source instead.
    # Building from source requires librdkafka to be installed.
    # Most platforms have an easy way to install librdkafka:
    #   - MacOS: `brew install librdkafka` gives latest, which is 1.9.x or newer.
    #   - Debian: `apt install librdkafka` gives 1.6.0 (https://packages.debian.org/bullseye/librdkafka-dev).
    #   - Ubuntu: `apt install librdkafka` gives 1.8.0 (https://launchpad.net/ubuntu/+source/librdkafka).
    #
    # Moreover, confluent_kafka 1.9.0 introduced a hard compatibility break, and
    # requires librdkafka >=1.9.0. As such, installing confluent_kafka 1.9.x on
    # most arm64 Linux machines will fail, since it will build from source but then
    # fail because librdkafka is too old. Hence, we have added an extra requirement
    # that requires confluent_kafka<1.9.0 on non-MacOS arm64/aarch64 machines, which
    # should ideally allow the builds to succeed in default conditions. We still
    # want to allow confluent_kafka >= 1.9.0 for M1 Macs, which is why we can't
    # broadly restrict confluent_kafka to <1.9.0.
    #
    # Note that this is somewhat of a hack, since we don't actually require the
    # older version of confluent_kafka on those machines. Additionally, we will
    # need monitor the Debian/Ubuntu PPAs and modify this rule if they start to
    # support librdkafka >= 1.9.0.
    "confluent_kafka>=1.5.0",
    'confluent_kafka<1.9.0; platform_system != "Darwin" and (platform_machine == "aarch64" or platform_machine == "arm64")',
    # We currently require both Avro libraries. The codegen uses avro-python3 (above)
    # schema parsers at runtime for generating and reading JSON into Python objects.
    # At the same time, we use Kafka's AvroSerializer, which internally relies on
    # fastavro for serialization. We do not use confluent_kafka[avro], since it
    # is incompatible with its own dep on avro-python3.
    "fastavro>=1.2.0",
}

kafka_protobuf = {
    "networkx>=2.6.2",
    # Required to generate protobuf python modules from the schema downloaded from the schema registry
    "grpcio==1.44.0",
    "grpcio-tools==1.44.0",
    "types-protobuf",
}

sql_common = {
    # Required for all SQL sources.
    "sqlalchemy==1.3.24",
    # Required for SQL profiling.
    "great-expectations>=0.15.12",
    # GE added handling for higher version of jinja2
    # https://github.com/great-expectations/great_expectations/pull/5382/files
    # datahub does not depend on traitlets directly but great expectations does.
    # https://github.com/ipython/traitlets/issues/741
    "traitlets<5.2.2",
    "greenlet",
}

aws_common = {
    # AWS Python SDK
    "boto3",
    # Deal with a version incompatibility between botocore (used by boto3) and urllib3.
    # See https://github.com/boto/botocore/pull/2563.
    "botocore!=1.23.0",
}

path_spec_common = {
    "parse>=1.19.0",
    "wcmatch",
}

looker_common = {
    # Looker Python SDK
    "looker-sdk==22.2.1"
}

bigquery_common = {
    # Google cloud logging library
    "google-cloud-logging<3.1.2",
    "google-cloud-bigquery",
    "more-itertools>=8.12.0",
    # we do not use protobuf directly but newer version caused bigquery connector to fail
    "protobuf<=3.20.1",
}

redshift_common = {
    "sqlalchemy-redshift",
    "psycopg2-binary",
    "GeoAlchemy2",
    "sqllineage==1.3.5",
    *path_spec_common,
}

snowflake_common = {
    # Snowflake plugin utilizes sql common
    *sql_common,
    # Required for all Snowflake sources
    "snowflake-sqlalchemy<=1.2.4",
    "cryptography",
    "msal",
}

trino = {
    "trino>=0.308",
    "trino[sqlalchemy]>=0.308",
}

microsoft_common = {"msal==1.16.0"}

iceberg_common = {
    # Iceberg Python SDK
    "acryl-iceberg-legacy==0.0.4",
    "azure-identity==1.10.0",
}

s3_base = {
    *aws_common,
    "parse>=1.19.0",
    "pyarrow>=6.0.1",
    "tableschema>=1.20.2",
    "ujson>=4.3.0",
    "types-ujson>=4.2.1",
    "smart-open[s3]>=5.2.1",
    "moto[s3]",
    *path_spec_common,
}

data_lake_profiling = {
    "pydeequ==1.0.1",
    "pyspark==3.0.3",
}

delta_lake = {
    *s3_base,
    "deltalake",
}

usage_common = {
    "sqlparse",
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
    "circuit-breaker": {
        "gql>=3.3.0",
        "gql[requests]>=3.3.0",
    },
    "great-expectations": sql_common | {"sqllineage==1.3.5"},
    # Source plugins
    # PyAthena is pinned with exact version because we use private method in PyAthena
    "athena": sql_common | {"PyAthena[SQLAlchemy]==2.4.1"},
    "azure-ad": set(),
    "bigquery": sql_common
    | bigquery_common
    | {"sqlalchemy-bigquery>=1.4.1", "sqllineage==1.3.5", "sqlparse"},
    "bigquery-usage": bigquery_common | usage_common | {"cachetools"},
    "clickhouse": sql_common | {"clickhouse-sqlalchemy==0.1.8"},
    "clickhouse-usage": sql_common
    | usage_common
    | {
        "clickhouse-sqlalchemy==0.1.8",
    },
    "datahub-lineage-file": set(),
    "datahub-business-glossary": set(),
    "delta-lake": {*data_lake_profiling, *delta_lake},
    "dbt": {"requests"} | aws_common,
    "druid": sql_common | {"pydruid>=0.6.2"},
    # Starting with 7.14.0 python client is checking if it is connected to elasticsearch client. If its not it throws
    # UnsupportedProductError
    # https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/release-notes.html#rn-7-14-0
    # https://github.com/elastic/elasticsearch-py/issues/1639#issuecomment-883587433
    "elasticsearch": {"elasticsearch==7.13.4"},
    "feast-legacy": {"docker"},
    "feast": {"feast==0.18.0", "flask-openid>=1.3.0"},
    "glue": aws_common,
    # hdbcli is supported officially by SAP, sqlalchemy-hana is built on top but not officially supported
    "hana": sql_common
    | {
        "sqlalchemy-hana>=0.5.0; platform_machine != 'aarch64' and platform_machine != 'arm64'",
        "hdbcli>=2.11.20; platform_machine != 'aarch64' and platform_machine != 'arm64'",
    },
    "hive": sql_common
    | {
        # Acryl Data maintains a fork of PyHive
        # - 0.6.11 adds support for table comments and column comments,
        #   and also releases HTTP and HTTPS transport schemes
        # - 0.6.12 adds support for Spark Thrift Server
        "acryl-pyhive[hive]>=0.6.13"
    },
    "iceberg": iceberg_common,
    "kafka": {*kafka_common, *kafka_protobuf},
    "kafka-connect": sql_common | {"requests", "JPype1"},
    "ldap": {"python-ldap>=2.4"},
    "looker": looker_common,
    # lkml>=1.1.2 is required to support the sql_preamble expression in LookML
    "lookml": looker_common
    | {"lkml>=1.1.2", "sql-metadata==2.2.2", "sqllineage==1.3.5"},
    "metabase": {"requests", "sqllineage==1.3.5"},
    "mode": {"requests", "sqllineage==1.3.5", "tenacity>=8.0.1"},
    "mongodb": {"pymongo>=3.11", "packaging"},
    "mssql": sql_common | {"sqlalchemy-pytds>=0.3"},
    "mssql-odbc": sql_common | {"pyodbc"},
    "mysql": sql_common | {"pymysql>=1.0.2"},
    # mariadb should have same dependency as mysql
    "mariadb": sql_common | {"pymysql>=1.0.2"},
    "okta": {"okta~=1.7.0"},
    "oracle": sql_common | {"cx_Oracle"},
    "postgres": sql_common | {"psycopg2-binary", "GeoAlchemy2"},
    "presto-on-hive": sql_common
    | {"psycopg2-binary", "acryl-pyhive[hive]>=0.6.12", "pymysql>=1.0.2"},
    "pulsar": {"requests"},
    "redash": {"redash-toolbelt", "sql-metadata", "sqllineage==1.3.5"},
    "redshift": sql_common | redshift_common,
    "redshift-usage": sql_common | usage_common | redshift_common,
    "s3": {*s3_base, *data_lake_profiling},
    "sagemaker": aws_common,
    "salesforce": {"simple-salesforce"},
    "snowflake": snowflake_common,
    "snowflake-usage": snowflake_common
    | usage_common
    | {
        "more-itertools>=8.12.0",
    },
    "snowflake-beta": snowflake_common | usage_common,
    "sqlalchemy": sql_common,
    "superset": {
        "requests",
        "sqlalchemy",
        "great_expectations",
        "greenlet",
        "Jinja2<3.1.0",
    },
    "tableau": {"tableauserverclient>=0.17.0"},
    "trino": sql_common | trino,
    "starburst-trino-usage": sql_common | usage_common | trino,
    "nifi": {"requests", "packaging"},
    "powerbi": microsoft_common,
    "vertica": sql_common | {"sqlalchemy-vertica[vertica-python]==0.0.5"},
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
    # avrogen package requires this
    "types-pytz",
    "types-pyOpenSSL",
}

base_dev_requirements = {
    *base_requirements,
    *framework_common,
    *mypy_stubs,
    *s3_base,
    "black>=21.12b0",
    "coverage>=5.1",
    "flake8>=3.8.3",
    "flake8-tidy-imports>=4.3.0",
    "isort>=5.7.0",
    "mypy>=0.950",
    # pydantic 1.8.2 is incompatible with mypy 0.910.
    # See https://github.com/samuelcolvin/pydantic/pull/3175#issuecomment-995382910.
    "pydantic>=1.9.0",
    "pytest>=6.2.2",
    "pytest-asyncio>=0.16.0",
    "pytest-cov>=2.8.1",
    "pytest-docker>=0.10.3,<0.12",
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
            "clickhouse",
            "clickhouse-usage",
            "delta-lake",
            "druid",
            "elasticsearch",
            "iceberg",
            "ldap",
            "looker",
            "lookml",
            "glue",
            "mariadb",
            "okta",
            "oracle",
            "postgres",
            "sagemaker",
            "kafka",
            "datahub-rest",
            "redash",
            "redshift",
            "redshift-usage",
            "s3",
            "tableau",
            "trino",
            "hive",
            "starburst-trino-usage",
            "powerbi",
            "vertica",
            "salesforce"
            # airflow is added below
        ]
        for dependency in plugins[plugin]
    ),
}

base_dev_requirements_airflow_1 = base_dev_requirements.copy()

base_dev_requirements = base_dev_requirements.union(
    # The feast plugin is not compatible with Airflow 1, so we add it later.
    {
        dependency
        for plugin in [
            "feast",
        ]
        for dependency in plugins[plugin]
    }
)


dev_requirements = {
    *base_dev_requirements,
    "apache-airflow[snowflake]>=2.0.2",  # snowflake is used in example dags
    "snowflake-sqlalchemy<=1.2.4",  # make constraint consistent with extras
}
dev_requirements_airflow_1_base = {
    "apache-airflow==1.10.15",
    "apache-airflow-backport-providers-snowflake",
    "snowflake-sqlalchemy<=1.2.4",  # make constraint consistent with extras
    "WTForms==2.3.3",  # make constraint consistent with extras
}
dev_requirements_airflow_1 = {
    *base_dev_requirements_airflow_1,
    *dev_requirements_airflow_1_base,
}

full_test_dev_requirements = {
    *list(
        dependency
        for plugin in [
            "athena",
            "circuit-breaker",
            "clickhouse",
            "delta-lake",
            "druid",
            "feast",
            "feast-legacy",
            "hana",
            "hive",
            "iceberg",
            "kafka-connect",
            "ldap",
            "mongodb",
            "mssql",
            "mysql",
            "mariadb",
            "snowflake",
            "redash",
            "vertica",
        ]
        for dependency in plugins[plugin]
    ),
}

entry_points = {
    "console_scripts": ["datahub = datahub.entrypoints:main"],
    "datahub.ingestion.source.plugins": [
        "csv-enricher = datahub.ingestion.source.csv_enricher:CSVEnricherSource",
        "file = datahub.ingestion.source.file:GenericFileSource",
        "sqlalchemy = datahub.ingestion.source.sql.sql_generic:SQLAlchemyGenericSource",
        "athena = datahub.ingestion.source.sql.athena:AthenaSource",
        "azure-ad = datahub.ingestion.source.identity.azure_ad:AzureADSource",
        "bigquery = datahub.ingestion.source.sql.bigquery:BigQuerySource",
        "bigquery-usage = datahub.ingestion.source.usage.bigquery_usage:BigQueryUsageSource",
        "clickhouse = datahub.ingestion.source.sql.clickhouse:ClickHouseSource",
        "clickhouse-usage = datahub.ingestion.source.usage.clickhouse_usage:ClickHouseUsageSource",
        "delta-lake = datahub.ingestion.source.delta_lake:DeltaLakeSource",
        "s3 = datahub.ingestion.source.s3:S3Source",
        "dbt = datahub.ingestion.source.dbt:DBTSource",
        "druid = datahub.ingestion.source.sql.druid:DruidSource",
        "elasticsearch = datahub.ingestion.source.elastic_search:ElasticsearchSource",
        "feast-legacy = datahub.ingestion.source.feast_legacy:FeastSource",
        "feast = datahub.ingestion.source.feast:FeastRepositorySource",
        "glue = datahub.ingestion.source.aws.glue:GlueSource",
        "sagemaker = datahub.ingestion.source.aws.sagemaker:SagemakerSource",
        "hana = datahub.ingestion.source.sql.hana:HanaSource",
        "hive = datahub.ingestion.source.sql.hive:HiveSource",
        "kafka = datahub.ingestion.source.kafka:KafkaSource",
        "kafka-connect = datahub.ingestion.source.kafka_connect:KafkaConnectSource",
        "ldap = datahub.ingestion.source.ldap:LDAPSource",
        "looker = datahub.ingestion.source.looker:LookerDashboardSource",
        "lookml = datahub.ingestion.source.lookml:LookMLSource",
        "datahub-lineage-file = datahub.ingestion.source.metadata.lineage:LineageFileSource",
        "datahub-business-glossary = datahub.ingestion.source.metadata.business_glossary:BusinessGlossaryFileSource",
        "mode = datahub.ingestion.source.mode:ModeSource",
        "mongodb = datahub.ingestion.source.mongodb:MongoDBSource",
        "mssql = datahub.ingestion.source.sql.mssql:SQLServerSource",
        "mysql = datahub.ingestion.source.sql.mysql:MySQLSource",
        "mariadb = datahub.ingestion.source.sql.mariadb.MariaDBSource",
        "okta = datahub.ingestion.source.identity.okta:OktaSource",
        "oracle = datahub.ingestion.source.sql.oracle:OracleSource",
        "postgres = datahub.ingestion.source.sql.postgres:PostgresSource",
        "redash = datahub.ingestion.source.redash:RedashSource",
        "redshift = datahub.ingestion.source.sql.redshift:RedshiftSource",
        "redshift-usage = datahub.ingestion.source.usage.redshift_usage:RedshiftUsageSource",
        "snowflake = datahub.ingestion.source.sql.snowflake:SnowflakeSource",
        "snowflake-usage = datahub.ingestion.source.usage.snowflake_usage:SnowflakeUsageSource",
        "snowflake-beta = datahub.ingestion.source.snowflake.snowflake_v2:SnowflakeV2Source",
        "superset = datahub.ingestion.source.superset:SupersetSource",
        "tableau = datahub.ingestion.source.tableau:TableauSource",
        "openapi = datahub.ingestion.source.openapi:OpenApiSource",
        "metabase = datahub.ingestion.source.metabase:MetabaseSource",
        "trino = datahub.ingestion.source.sql.trino:TrinoSource",
        "starburst-trino-usage = datahub.ingestion.source.usage.starburst_trino_usage:TrinoUsageSource",
        "nifi = datahub.ingestion.source.nifi:NifiSource",
        "powerbi = datahub.ingestion.source.powerbi:PowerBiDashboardSource",
        "iceberg = datahub.ingestion.source.iceberg.iceberg:IcebergSource",
        "vertica = datahub.ingestion.source.sql.vertica:VerticaSource",
        "presto-on-hive = datahub.ingestion.source.sql.presto_on_hive:PrestoOnHiveSource",
        "pulsar = datahub.ingestion.source.pulsar:PulsarSource",
        "salesforce = datahub.ingestion.source.salesforce:SalesforceSource",
    ],
    "datahub.ingestion.sink.plugins": [
        "file = datahub.ingestion.sink.file:FileSink",
        "console = datahub.ingestion.sink.console:ConsoleSink",
        "datahub-kafka = datahub.ingestion.sink.datahub_kafka:DatahubKafkaSink",
        "datahub-rest = datahub.ingestion.sink.datahub_rest:DatahubRestSink",
    ],
    "datahub.ingestion.checkpointing_provider.plugins": [
        "datahub = datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider:DatahubIngestionCheckpointingProvider",
    ],
    "datahub.ingestion.reporting_provider.plugins": [
        "datahub = datahub.ingestion.reporting.datahub_ingestion_run_summary_provider:DatahubIngestionRunSummaryProvider",
        "file = datahub.ingestion.reporting.file_reporter:FileReporter",
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
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
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
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
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
    python_requires=">=3.7",
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
        "dev-airflow1-base": list(dev_requirements_airflow_1_base),
        "dev-airflow1": list(dev_requirements_airflow_1),
        "integration-tests": list(full_test_dev_requirements),
    },
)
