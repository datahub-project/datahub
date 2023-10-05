import sys
from typing import Dict, Set

import setuptools

package_metadata: dict = {}
with open("./src/datahub/__init__.py") as fp:
    exec(fp.read(), package_metadata)


base_requirements = {
    # Typing extension should be >=3.10.0.2 ideally but we can't restrict due to a Airflow 2.1 dependency conflict.
    "typing_extensions>=3.7.4.3",
    "mypy_extensions>=0.4.3",
    # Actual dependencies.
    "typing-inspect",
    # pydantic 1.10.3 is incompatible with typing-extensions 4.1.1 - https://github.com/pydantic/pydantic/issues/4885
    # pydantic 2 makes major, backwards-incompatible changes - https://github.com/pydantic/pydantic/issues/4887
    "pydantic>=1.5.1,!=1.10.3,<2",
    "mixpanel>=4.9.0",
    "sentry-sdk",
}

framework_common = {
    "click>=7.1.2",
    "click-default-group",
    "PyYAML",
    "toml>=0.10.0",
    # In Python 3.10+, importlib_metadata is included in the standard library.
    "importlib_metadata>=4.0.0; python_version < '3.10'",
    "docker",
    "expandvars>=0.6.5",
    "avro-gen3==0.7.11",
    # "avro-gen3 @ git+https://github.com/acryldata/avro_gen@master#egg=avro-gen3",
    "avro>=1.10.2,<1.11",
    "python-dateutil>=2.8.0",
    "tabulate",
    "progressbar2",
    "termcolor>=1.0.0",
    "psutil>=5.8.0",
    "ratelimiter",
    "Deprecated",
    "humanfriendly",
    "packaging",
    "aiohttp<4",
    "cached_property",
    "ijson",
    "click-spinner",
    "requests_file",
    "jsonref",
    # jsonschema drops python 3.7 support in v4.18.0
    "jsonschema<=4.17.3; python_version < '3.8'",
    "jsonschema; python_version >= '3.8'",
    "ruamel.yaml",
}

rest_common = {"requests", "requests_file"}

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
    # NOTE: potential conflict with feast also depending on grpcio
    "grpcio>=1.44.0,<2",
    "grpcio-tools>=1.44.0,<2",
}

sql_common = {
    # Required for all SQL sources.
    # This is temporary lower bound that we're open to loosening/tightening as requirements show up
    "sqlalchemy>=1.4.39, <2",
    # Required for SQL profiling.
    "great-expectations>=0.15.12, <=0.15.50",
    # scipy version restricted to reduce backtracking, used by great-expectations,
    "scipy>=1.7.2",
    # GE added handling for higher version of jinja2
    # https://github.com/great-expectations/great_expectations/pull/5382/files
    # datahub does not depend on traitlets directly but great expectations does.
    # https://github.com/ipython/traitlets/issues/741
    "traitlets<5.2.2",
    "greenlet",
}

sqllineage_lib = {
    "sqllineage==1.3.8",
    # We don't have a direct dependency on sqlparse but it is a dependency of sqllineage.
    # There have previously been issues from not pinning sqlparse, so it's best to pin it.
    # Related: https://github.com/reata/sqllineage/issues/361 and https://github.com/reata/sqllineage/pull/360
    "sqlparse==0.4.4",
}

sqlglot_lib = {
    # Using an Acryl fork of sqlglot.
    # https://github.com/tobymao/sqlglot/compare/main...hsheth2:sqlglot:hsheth?expand=1
    "acryl-sqlglot==18.5.2.dev45",
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
    "looker-sdk==23.0.0",
    # This version of lkml contains a fix for parsing lists in
    # LookML files with spaces between an item and the following comma.
    # See https://github.com/joshtemple/lkml/issues/73.
    "lkml>=1.3.0b5",
    "sql-metadata==2.2.2",
    *sqllineage_lib,
    "GitPython>2",
}

bigquery_common = {
    # Google cloud logging library
    "google-cloud-logging<=3.5.0",
    "google-cloud-bigquery",
    "more-itertools>=8.12.0",
}

clickhouse_common = {
    # Clickhouse 0.2.0 adds support for SQLAlchemy 1.4.x
    "clickhouse-sqlalchemy>=0.2.0",
}

redshift_common = {
    # Clickhouse 0.8.3 adds support for SQLAlchemy 1.4.x
    "sqlalchemy-redshift>=0.8.3",
    "psycopg2-binary",
    "GeoAlchemy2",
    *sqllineage_lib,
    *path_spec_common,
}

snowflake_common = {
    # Snowflake plugin utilizes sql common
    *sql_common,
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/350
    "snowflake-sqlalchemy>=1.4.3",
    # See https://github.com/snowflakedb/snowflake-connector-python/pull/1348 for why 2.8.2 is blocked
    "snowflake-connector-python!=2.8.2",
    "pandas",
    "cryptography",
    "msal",
    "acryl-datahub-classify==0.0.8",
    # spacy version restricted to reduce backtracking, used by acryl-datahub-classify,
    "spacy==3.4.3",
}

trino = {
    "trino[sqlalchemy]>=0.308",
}

pyhive_common = {
    # Acryl Data maintains a fork of PyHive
    # - 0.6.11 adds support for table comments and column comments,
    #   and also releases HTTP and HTTPS transport schemes
    # - 0.6.12 adds support for Spark Thrift Server
    # - 0.6.13 adds a small fix for Databricks
    # - 0.6.14 uses pure-sasl instead of sasl so it builds on Python 3.11
    "acryl-pyhive[hive_pure_sasl]==0.6.14",
    # As per https://github.com/datahub-project/datahub/issues/8405
    # and https://github.com/dropbox/PyHive/issues/417, new versions
    # of thrift break PyHive's hive+http transport.
    "thrift<0.14.0",
}

microsoft_common = {"msal==1.22.0"}

iceberg_common = {
    # Iceberg Python SDK
    "pyiceberg",
    "pyarrow>=9.0.0, <13.0.0",
}

s3_base = {
    *aws_common,
    "more-itertools>=8.12.0",
    "parse>=1.19.0",
    "pyarrow>=6.0.1",
    "tableschema>=1.20.2",
    # ujson 5.2.0 has the JSONDecodeError exception type, which we need for error handling.
    "ujson>=5.2.0",
    "smart-open[s3]>=5.2.1",
    "moto[s3]",
    *path_spec_common,
}

data_lake_profiling = {
    "pydeequ==1.1.0",
    "pyspark~=3.3.0",
}

delta_lake = {
    *s3_base,
    "deltalake>=0.6.3, != 0.6.4",
}

powerbi_report_server = {"requests", "requests_ntlm"}

usage_common = {
    "sqlparse",
}

databricks = {
    # 0.1.11 appears to have authentication issues with azure databricks
    "databricks-sdk>=0.9.0",
    "pyspark",
    "requests",
}

mysql = sql_common | {"pymysql>=1.0.2"}

# Note: for all of these, framework_common will be added.
plugins: Dict[str, Set[str]] = {
    # Sink plugins.
    "datahub-kafka": kafka_common,
    "datahub-rest": rest_common,
    "sync-file-emitter": {"filelock"},
    "datahub-lite": {
        "duckdb",
        "fastapi",
        "uvicorn",
    },
    # Integrations.
    "airflow": {
        f"acryl-datahub-airflow-plugin == {package_metadata['__version__']}",
    },
    "circuit-breaker": {
        "gql>=3.3.0",
        "gql[requests]>=3.3.0",
    },
    "datahub": mysql | kafka_common,
    "great-expectations": sql_common | sqllineage_lib,
    # Misc plugins.
    "sql-parser": sqlglot_lib,
    # Source plugins
    # PyAthena is pinned with exact version because we use private method in PyAthena
    "athena": sql_common | {"PyAthena[SQLAlchemy]==2.4.1"},
    "azure-ad": set(),
    "bigquery": sql_common
    | bigquery_common
    | {
        # TODO: I doubt we need all three sql parsing libraries.
        *sqllineage_lib,
        *sqlglot_lib,
        "sqlalchemy-bigquery>=1.4.1",
        "google-cloud-datacatalog-lineage==0.2.2",
    },
    "clickhouse": sql_common | clickhouse_common,
    "clickhouse-usage": sql_common | usage_common | clickhouse_common,
    "datahub-lineage-file": set(),
    "datahub-business-glossary": set(),
    "delta-lake": {*data_lake_profiling, *delta_lake},
    "dbt": {"requests"} | aws_common,
    "dbt-cloud": {"requests"},
    "druid": sql_common | {"pydruid>=0.6.2"},
    "dynamodb": aws_common,
    # Starting with 7.14.0 python client is checking if it is connected to elasticsearch client. If its not it throws
    # UnsupportedProductError
    # https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/release-notes.html#rn-7-14-0
    # https://github.com/elastic/elasticsearch-py/issues/1639#issuecomment-883587433
    "elasticsearch": {"elasticsearch==7.13.4"},
    "feast": {
        "feast~=0.31.1",
        "flask-openid>=1.3.0",
        # typeguard 3.x, released on 2023-03-14, seems to cause issues with Feast.
        "typeguard<3",
    },
    "glue": aws_common,
    # hdbcli is supported officially by SAP, sqlalchemy-hana is built on top but not officially supported
    "hana": sql_common
    | {
        "sqlalchemy-hana>=0.5.0; platform_machine != 'aarch64' and platform_machine != 'arm64'",
        "hdbcli>=2.11.20; platform_machine != 'aarch64' and platform_machine != 'arm64'",
    },
    "hive": sql_common
    | pyhive_common
    | {
        "databricks-dbapi",
        # Due to https://github.com/great-expectations/great_expectations/issues/6146,
        # we cannot allow 0.15.{23-26}. This was fixed in 0.15.27 by
        # https://github.com/great-expectations/great_expectations/pull/6149.
        "great-expectations != 0.15.23, != 0.15.24, != 0.15.25, != 0.15.26",
    },
    "iceberg": iceberg_common,
    "json-schema": set(),
    "kafka": kafka_common | kafka_protobuf,
    "kafka-connect": sql_common | {"requests", "JPype1"},
    "ldap": {"python-ldap>=2.4"},
    "looker": looker_common,
    "lookml": looker_common,
    "metabase": {"requests"} | sqllineage_lib,
    "mlflow": {"mlflow-skinny>=2.3.0"},
    "mode": {"requests", "tenacity>=8.0.1"} | sqllineage_lib,
    "mongodb": {"pymongo[srv]>=3.11", "packaging"},
    "mssql": sql_common | {"sqlalchemy-pytds>=0.3"},
    "mssql-odbc": sql_common | {"pyodbc"},
    "mysql": mysql,
    # mariadb should have same dependency as mysql
    "mariadb": sql_common | {"pymysql>=1.0.2"},
    "okta": {"okta~=1.7.0", "nest-asyncio"},
    "oracle": sql_common | {"cx_Oracle"},
    "postgres": sql_common | {"psycopg2-binary", "GeoAlchemy2"},
    "presto": sql_common | pyhive_common | trino,
    "presto-on-hive": sql_common
    | pyhive_common
    | {"psycopg2-binary", "pymysql>=1.0.2"},
    "pulsar": {"requests"},
    "redash": {"redash-toolbelt", "sql-metadata"} | sqllineage_lib,
    "redshift": sql_common | redshift_common | usage_common | {"redshift-connector"},
    "redshift-legacy": sql_common | redshift_common,
    "redshift-usage-legacy": sql_common | usage_common | redshift_common,
    "s3": {*s3_base, *data_lake_profiling},
    "gcs": {*s3_base, *data_lake_profiling},
    "sagemaker": aws_common,
    "salesforce": {"simple-salesforce"},
    "snowflake": snowflake_common | usage_common | sqlglot_lib,
    "sqlalchemy": sql_common,
    "sql-queries": usage_common | sqlglot_lib,
    "superset": {
        "requests",
        "sqlalchemy",
        "great_expectations",
        "greenlet",
    },
    # FIXME: I don't think tableau uses sqllineage anymore so we should be able
    # to remove that dependency.
    "tableau": {"tableauserverclient>=0.17.0"} | sqllineage_lib | sqlglot_lib,
    "trino": sql_common | trino,
    "starburst-trino-usage": sql_common | usage_common | trino,
    "nifi": {"requests", "packaging", "requests-gssapi"},
    "powerbi": microsoft_common | {"lark[regex]==1.1.4", "sqlparse"} | sqlglot_lib,
    "powerbi-report-server": powerbi_report_server,
    "vertica": sql_common | {"vertica-sqlalchemy-dialect[vertica-python]==0.0.8"},
    "unity-catalog": databricks | sqllineage_lib,
}

# This is mainly used to exclude plugins from the Docker image.
all_exclude_plugins: Set[str] = {
    # SQL Server ODBC requires additional drivers, and so we don't want to keep
    # it included in the default "all" installation.
    "mssql-odbc",
    # duckdb doesn't have a prebuilt wheel for Linux arm7l or aarch64, so we
    # simply exclude it.
    "datahub-lite",
}

mypy_stubs = {
    "types-dataclasses",
    "types-pkg_resources",
    "types-six",
    "types-python-dateutil",
    # We need to avoid 2.31.0.5 and 2.31.0.4 due to
    # https://github.com/python/typeshed/issues/10764. Once that
    # issue is resolved, we can remove the upper bound and change it
    # to a != constraint.
    # We have a PR up to fix the underlying issue: https://github.com/python/typeshed/pull/10776.
    "types-requests>=2.28.11.6,<=2.31.0.3",
    "types-toml",
    "types-PyMySQL",
    "types-PyYAML",
    "types-freezegun",
    "types-cachetools",
    # versions 0.1.13 and 0.1.14 seem to have issues
    "types-click==0.1.12",
    # The boto3-stubs package seems to have regularly breaking minor releases,
    # we pin to a specific version to avoid this.
    "boto3-stubs[s3,glue,sagemaker,sts]==1.28.15",
    "mypy-boto3-sagemaker==1.28.15",  # For some reason, above pin only restricts `mypy-boto3-sagemaker<1.29.0,>=1.28.0`
    "types-tabulate",
    # avrogen package requires this
    "types-pytz",
    "types-pyOpenSSL",
    "types-click-spinner>=0.1.13.1",
    "types-ujson>=5.2.0",
    "types-termcolor>=1.0.0",
    "types-Deprecated",
    "types-protobuf>=4.21.0.1",
    "sqlalchemy2-stubs",
}


pytest_dep = "pytest>=6.2.2"
deepdiff_dep = "deepdiff"
test_api_requirements = {pytest_dep, deepdiff_dep, "PyYAML"}

base_dev_requirements = {
    *base_requirements,
    *framework_common,
    *mypy_stubs,
    *s3_base,
    # This is pinned only to avoid spurious errors in CI.
    # We should make an effort to keep it up to date.
    "black==22.12.0",
    "coverage>=5.1",
    "faker>=18.4.0",
    "flake8>=3.8.3",  # DEPRECATION: Once we drop Python 3.7, we can pin to 6.x.
    "flake8-tidy-imports>=4.3.0",
    "flake8-bugbear==23.3.12",
    "isort>=5.7.0",
    "mypy==1.0.0",
    # pydantic 1.8.2 is incompatible with mypy 0.910.
    # See https://github.com/samuelcolvin/pydantic/pull/3175#issuecomment-995382910.
    "pydantic>=1.10.0",
    *test_api_requirements,
    pytest_dep,
    "pytest-asyncio>=0.16.0",
    "pytest-cov>=2.8.1",
    "pytest-docker>=1.0.1",
    deepdiff_dep,
    "requests-mock",
    "freezegun",
    "jsonpickle",
    "build",
    "twine",
    *list(
        dependency
        for plugin in [
            "athena",
            "bigquery",
            "clickhouse",
            "clickhouse-usage",
            "delta-lake",
            "druid",
            "elasticsearch",
            "feast" if sys.version_info >= (3, 8) else None,
            "iceberg" if sys.version_info >= (3, 8) else None,
            "mlflow" if sys.version_info >= (3, 8) else None,
            "json-schema",
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
            "datahub-lite",
            "great-expectations",
            "presto",
            "redash",
            "redshift",
            "redshift-legacy",
            "redshift-usage-legacy",
            "s3",
            "snowflake",
            "tableau",
            "trino",
            "hive",
            "starburst-trino-usage",
            "powerbi",
            "powerbi-report-server",
            "salesforce",
            "unity-catalog",
            "nifi",
            "vertica",
            "mode",
            "kafka-connect",
        ]
        if plugin
        for dependency in plugins[plugin]
    ),
}

dev_requirements = {
    *base_dev_requirements,
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
            "feast" if sys.version_info >= (3, 8) else None,
            "hana",
            "hive",
            "iceberg" if sys.version_info >= (3, 8) else None,
            "kafka-connect",
            "ldap",
            "mongodb",
            "mssql",
            "mysql",
            "mariadb",
            "redash",
            "vertica",
        ]
        if plugin
        for dependency in plugins[plugin]
    ),
}

entry_points = {
    "console_scripts": ["datahub = datahub.entrypoints:main"],
    "datahub.ingestion.source.plugins": [
        "csv-enricher = datahub.ingestion.source.csv_enricher:CSVEnricherSource",
        "file = datahub.ingestion.source.file:GenericFileSource",
        "datahub = datahub.ingestion.source.datahub.datahub_source:DataHubSource",
        "sqlalchemy = datahub.ingestion.source.sql.sql_generic:SQLAlchemyGenericSource",
        "athena = datahub.ingestion.source.sql.athena:AthenaSource",
        "azure-ad = datahub.ingestion.source.identity.azure_ad:AzureADSource",
        "bigquery = datahub.ingestion.source.bigquery_v2.bigquery:BigqueryV2Source",
        "clickhouse = datahub.ingestion.source.sql.clickhouse:ClickHouseSource",
        "clickhouse-usage = datahub.ingestion.source.usage.clickhouse_usage:ClickHouseUsageSource",
        "delta-lake = datahub.ingestion.source.delta_lake:DeltaLakeSource",
        "s3 = datahub.ingestion.source.s3:S3Source",
        "dbt = datahub.ingestion.source.dbt.dbt_core:DBTCoreSource",
        "dbt-cloud = datahub.ingestion.source.dbt.dbt_cloud:DBTCloudSource",
        "druid = datahub.ingestion.source.sql.druid:DruidSource",
        "dynamodb = datahub.ingestion.source.dynamodb.dynamodb:DynamoDBSource",
        "elasticsearch = datahub.ingestion.source.elastic_search:ElasticsearchSource",
        "feast = datahub.ingestion.source.feast:FeastRepositorySource",
        "glue = datahub.ingestion.source.aws.glue:GlueSource",
        "sagemaker = datahub.ingestion.source.aws.sagemaker:SagemakerSource",
        "hana = datahub.ingestion.source.sql.hana:HanaSource",
        "hive = datahub.ingestion.source.sql.hive:HiveSource",
        "json-schema = datahub.ingestion.source.schema.json_schema:JsonSchemaSource",
        "kafka = datahub.ingestion.source.kafka:KafkaSource",
        "kafka-connect = datahub.ingestion.source.kafka_connect:KafkaConnectSource",
        "ldap = datahub.ingestion.source.ldap:LDAPSource",
        "looker = datahub.ingestion.source.looker.looker_source:LookerDashboardSource",
        "lookml = datahub.ingestion.source.looker.lookml_source:LookMLSource",
        "datahub-lineage-file = datahub.ingestion.source.metadata.lineage:LineageFileSource",
        "datahub-business-glossary = datahub.ingestion.source.metadata.business_glossary:BusinessGlossaryFileSource",
        "mlflow = datahub.ingestion.source.mlflow:MLflowSource",
        "mode = datahub.ingestion.source.mode:ModeSource",
        "mongodb = datahub.ingestion.source.mongodb:MongoDBSource",
        "mssql = datahub.ingestion.source.sql.mssql:SQLServerSource",
        "mysql = datahub.ingestion.source.sql.mysql:MySQLSource",
        "mariadb = datahub.ingestion.source.sql.mariadb.MariaDBSource",
        "okta = datahub.ingestion.source.identity.okta:OktaSource",
        "oracle = datahub.ingestion.source.sql.oracle:OracleSource",
        "postgres = datahub.ingestion.source.sql.postgres:PostgresSource",
        "redash = datahub.ingestion.source.redash:RedashSource",
        "redshift = datahub.ingestion.source.redshift.redshift:RedshiftSource",
        "redshift-legacy = datahub.ingestion.source.sql.redshift:RedshiftSource",
        "redshift-usage-legacy = datahub.ingestion.source.usage.redshift_usage:RedshiftUsageSource",
        "snowflake = datahub.ingestion.source.snowflake.snowflake_v2:SnowflakeV2Source",
        "superset = datahub.ingestion.source.superset:SupersetSource",
        "tableau = datahub.ingestion.source.tableau:TableauSource",
        "openapi = datahub.ingestion.source.openapi:OpenApiSource",
        "metabase = datahub.ingestion.source.metabase:MetabaseSource",
        "trino = datahub.ingestion.source.sql.trino:TrinoSource",
        "starburst-trino-usage = datahub.ingestion.source.usage.starburst_trino_usage:TrinoUsageSource",
        "nifi = datahub.ingestion.source.nifi:NifiSource",
        "powerbi = datahub.ingestion.source.powerbi:PowerBiDashboardSource",
        "powerbi-report-server = datahub.ingestion.source.powerbi_report_server:PowerBiReportServerDashboardSource",
        "iceberg = datahub.ingestion.source.iceberg.iceberg:IcebergSource",
        "vertica = datahub.ingestion.source.sql.vertica:VerticaSource",
        "presto = datahub.ingestion.source.sql.presto:PrestoSource",
        "presto-on-hive = datahub.ingestion.source.sql.presto_on_hive:PrestoOnHiveSource",
        "pulsar = datahub.ingestion.source.pulsar:PulsarSource",
        "salesforce = datahub.ingestion.source.salesforce:SalesforceSource",
        "demo-data = datahub.ingestion.source.demo_data.DemoDataSource",
        "unity-catalog = datahub.ingestion.source.unity.source:UnityCatalogSource",
        "gcs = datahub.ingestion.source.gcs.gcs_source:GCSSource",
        "sql-queries = datahub.ingestion.source.sql_queries:SqlQueriesSource",
    ],
    "datahub.ingestion.transformer.plugins": [
        "simple_remove_dataset_ownership = datahub.ingestion.transformer.remove_dataset_ownership:SimpleRemoveDatasetOwnership",
        "mark_dataset_status = datahub.ingestion.transformer.mark_dataset_status:MarkDatasetStatus",
        "set_dataset_browse_path = datahub.ingestion.transformer.add_dataset_browse_path:AddDatasetBrowsePathTransformer",
        "add_dataset_ownership = datahub.ingestion.transformer.add_dataset_ownership:AddDatasetOwnership",
        "simple_add_dataset_ownership = datahub.ingestion.transformer.add_dataset_ownership:SimpleAddDatasetOwnership",
        "pattern_add_dataset_ownership = datahub.ingestion.transformer.add_dataset_ownership:PatternAddDatasetOwnership",
        "add_dataset_domain = datahub.ingestion.transformer.dataset_domain:AddDatasetDomain",
        "simple_add_dataset_domain = datahub.ingestion.transformer.dataset_domain:SimpleAddDatasetDomain",
        "pattern_add_dataset_domain = datahub.ingestion.transformer.dataset_domain:PatternAddDatasetDomain",
        "add_dataset_tags = datahub.ingestion.transformer.add_dataset_tags:AddDatasetTags",
        "simple_add_dataset_tags = datahub.ingestion.transformer.add_dataset_tags:SimpleAddDatasetTags",
        "pattern_add_dataset_tags = datahub.ingestion.transformer.add_dataset_tags:PatternAddDatasetTags",
        "extract_dataset_tags = datahub.ingestion.transformer.extract_dataset_tags:ExtractDatasetTags",
        "add_dataset_terms = datahub.ingestion.transformer.add_dataset_terms:AddDatasetTerms",
        "simple_add_dataset_terms = datahub.ingestion.transformer.add_dataset_terms:SimpleAddDatasetTerms",
        "pattern_add_dataset_terms = datahub.ingestion.transformer.add_dataset_terms:PatternAddDatasetTerms",
        "add_dataset_properties = datahub.ingestion.transformer.add_dataset_properties:AddDatasetProperties",
        "simple_add_dataset_properties = datahub.ingestion.transformer.add_dataset_properties:SimpleAddDatasetProperties",
        "pattern_add_dataset_schema_terms = datahub.ingestion.transformer.add_dataset_schema_terms:PatternAddDatasetSchemaTerms",
        "pattern_add_dataset_schema_tags = datahub.ingestion.transformer.add_dataset_schema_tags:PatternAddDatasetSchemaTags",
        "extract_owners_from_tags = datahub.ingestion.transformer.extract_ownership_from_tags:ExtractOwnersFromTagsTransformer",
    ],
    "datahub.ingestion.sink.plugins": [
        "file = datahub.ingestion.sink.file:FileSink",
        "console = datahub.ingestion.sink.console:ConsoleSink",
        "blackhole = datahub.ingestion.sink.blackhole:BlackHoleSink",
        "datahub-kafka = datahub.ingestion.sink.datahub_kafka:DatahubKafkaSink",
        "datahub-rest = datahub.ingestion.sink.datahub_rest:DatahubRestSink",
        "datahub-lite = datahub.ingestion.sink.datahub_lite:DataHubLiteSink",
    ],
    "datahub.ingestion.checkpointing_provider.plugins": [
        "datahub = datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider:DatahubIngestionCheckpointingProvider",
    ],
    "datahub.ingestion.reporting_provider.plugins": [
        "datahub = datahub.ingestion.reporting.datahub_ingestion_run_summary_provider:DatahubIngestionRunSummaryProvider",
        "file = datahub.ingestion.reporting.file_reporter:FileReporter",
    ],
    "datahub.custom_packages": [],
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
    long_description="""\
The `acryl-datahub` package contains a CLI and SDK for interacting with DataHub,
as well as an integration framework for pulling/pushing metadata from external systems.

See the [DataHub docs](https://datahubproject.io/docs/metadata-ingestion).
""",
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
        "datahub.ingestion.source.powerbi": ["powerbi-lexical-grammar.rule"],
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
        "cloud": ["acryl-datahub-cloud"],
        "dev": list(dev_requirements),
        "testing-utils": list(test_api_requirements),  # To import `datahub.testing`
        "integration-tests": list(full_test_dev_requirements),
    },
)
