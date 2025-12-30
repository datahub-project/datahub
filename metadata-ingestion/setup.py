from typing import Dict, Set

import setuptools

package_metadata: dict = {}
with open("./src/datahub/_version.py") as fp:
    exec(fp.read(), package_metadata)

_version: str = package_metadata["__version__"]
_self_pin = (
    f"=={_version}"
    if not (_version.endswith(("dev0", "dev1")) or "docker" in _version)
    else ""
)

base_requirements = {
    # Our min version of typing_extensions is somewhat constrained by Airflow.
    "typing_extensions>=4.8.0",
    # Actual dependencies.
    "typing-inspect",
    "pydantic>=2.4.0,<3.0.0",
    # 2.41.3 https://github.com/pydantic/pydantic-core/issues/1841
    "pydantic_core!=2.41.3",
    "mixpanel>=4.9.0",
    # Airflow depends on fairly old versions of sentry-sdk, which is why we need to be loose with our constraints.
    "sentry-sdk>=1.33.1",
}

framework_common = {
    # Avoiding click 8.2.0 due to https://github.com/pallets/click/issues/2894
    "click>=7.1.2, !=8.2.0",
    "click-default-group",
    "PyYAML",
    "toml>=0.10.0",
    # In Python 3.10+, importlib_metadata is included in the standard library.
    "importlib_metadata>=4.0.0; python_version < '3.10'",
    "docker",
    "expandvars>=0.6.5",
    "avro-gen3==0.7.16",
    # "avro-gen3 @ git+https://github.com/acryldata/avro_gen@master#egg=avro-gen3",
    # avro has historically made breaking changes, so we have a cautious upper bound.
    "avro>=1.11.3,<1.13",
    "python-dateutil>=2.8.0",
    "tabulate",
    "progressbar2",
    "psutil>=5.8.0",
    "Deprecated",
    "humanfriendly",
    "packaging",
    "aiohttp<4",
    "cached_property",
    "ijson",
    "click-spinner",
    "requests_file",
    "jsonref",
    "jsonschema",
    "ruamel.yaml",
}

rest_common = {"requests", "requests_file"}

kafka_common = {
    # Note that confluent_kafka 1.9.0 introduced a hard compatibility break, and
    # requires librdkafka >=1.9.0. This is generally not an issue, since they
    # now provide prebuilt wheels for most platforms, including M1 Macs and
    # Linux aarch64 (e.g. Docker's linux/arm64). Installing confluent_kafka
    # from source remains a pain.
    # With the release of 2.8.1, confluent-kafka only released a source distribution,
    # and no prebuilt wheels.
    # See https://github.com/confluentinc/confluent-kafka-python/issues/1927
    # RegisteredSchema#guid is being used and was introduced in 2.10.1 https://github.com/confluentinc/confluent-kafka-python/pull/1978
    "confluent_kafka[schemaregistry,avro]>=2.10.1",
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

usage_common = {
    "sqlparse",
}

sqlglot_lib = {
    # We heavily monkeypatch sqlglot.
    # We used to maintain an acryl-sqlglot fork: https://github.com/tobymao/sqlglot/compare/main...hsheth2:sqlglot:main?expand=1
    # but not longer do.
    "sqlglot[rs]==27.27.0",
    "patchy==2.8.0",
}

classification_lib = {
    "acryl-datahub-classify==0.0.11",
    # schwifty is needed for the classify plugin
    "schwifty",
    # This is a bit of a hack. Because we download the SpaCy model at runtime in the classify plugin,
    # we need pip to be available.
    "pip",
    # We were seeing an error like this `numpy.dtype size changed, may indicate binary incompatibility. Expected 96 from C header, got 88 from PyObject`
    # with numpy 2.0. This likely indicates a mismatch between scikit-learn and numpy versions.
    # https://stackoverflow.com/questions/40845304/runtimewarning-numpy-dtype-size-changed-may-indicate-binary-incompatibility
    "numpy<2",
}

dbt_common = {
    *sqlglot_lib,
    "more-itertools",
}

cachetools_lib = {
    "cachetools",
}

# Skip pyarrow 0.14.0-14.0.0 due to CVE-2023-47248: https://avd.aquasec.com/nvd/cve-2023-47248
pyarrow_common = {
    "pyarrow>14.0.0",
}

great_expectations_lib = {
    # 1. Our original dep was this:
    # "great-expectations>=0.15.12, <=0.15.50",
    # 2. For hive, we had additional restrictions:
    #    Due to https://github.com/great-expectations/great_expectations/issues/6146,
    #    we cannot allow 0.15.{23-26}. This was fixed in 0.15.27 by
    #    https://github.com/great-expectations/great_expectations/pull/6149.
    # "great-expectations != 0.15.23, != 0.15.24, != 0.15.25, != 0.15.26",
    # 3. Since then, we've ended up forking great-expectations in order to
    #    add pydantic 2.x support. The fork is pretty simple
    #    https://github.com/great-expectations/great_expectations/compare/0.15.50...acryldata:great_expectations:0.15.50-pydantic-2-patch?expand=1
    #    This was derived from work done by @jskrzypek in
    #    https://github.com/datahub-project/datahub/issues/8115#issuecomment-2264219783
    "acryl-great-expectations==0.15.50.1",
    "jupyter_server>=2.14.1",  # CVE-2024-35178
}

sqlalchemy_lib = {
    # Required for all SQL sources.
    # This is temporary lower bound that we're open to loosening/tightening as requirements show up
    "sqlalchemy>=1.4.39, <2",
}
sql_common = (
    {
        *sqlalchemy_lib,
        # Required for SQL profiling.
        *great_expectations_lib,
        # scipy version restricted to reduce backtracking, used by great-expectations,
        "scipy>=1.7.2",
        # GE added handling for higher version of jinja2
        # https://github.com/great-expectations/great_expectations/pull/5382/files
        # datahub does not depend on traitlets directly but great expectations does.
        # https://github.com/ipython/traitlets/issues/741
        "traitlets!=5.2.2",
        # GE depends on IPython - we have no direct dependency on it.
        # IPython 8.22.0 added a dependency on traitlets 5.13.x, but only declared a
        # version requirement of traitlets>5.
        # See https://github.com/ipython/ipython/issues/14352.
        # This issue was fixed by https://github.com/ipython/ipython/pull/14353,
        # which first appeared in IPython 8.22.1.
        # As such, we just need to avoid that version in order to get the
        # dependencies that we need. IPython probably should've yanked 8.22.0.
        "IPython!=8.22.0",
        "greenlet",
        *cachetools_lib,
    }
    | usage_common
    | sqlglot_lib
    | classification_lib
)

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
    "looker-sdk>=23.0.0",
    # This version of lkml contains a fix for parsing lists in
    # LookML files with spaces between an item and the following comma.
    # See https://github.com/joshtemple/lkml/issues/73.
    "lkml>=1.3.4",
    *sqlglot_lib,
    "GitPython>2",
    # python-liquid 2 includes a bunch of breaking changes.
    # See https://jg-rp.github.io/liquid/migration/
    # Eventually we should fully upgrade to v2, but that will require
    # us to drop Python 3.8 support first.
    "python-liquid<2",
    "deepmerge>=1.1.1",
}

bigquery_common = {
    # Google cloud logging library
    "google-cloud-logging<=3.5.0",
    "google-cloud-bigquery",
    "google-cloud-datacatalog>=1.5.0",
    "google-cloud-resource-manager",
    "more-itertools>=8.12.0",
    "sqlalchemy-bigquery>=1.4.1",
    *path_spec_common,
}

clickhouse_common = {
    # Clickhouse 0.2.0 adds support for SQLAlchemy 1.4.x
    # Disallow 0.2.5 because of https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/272.
    # Note that there's also a known issue around nested map types: https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/269.
    # zstd needs to be pinned because the latest version causes issues on arm
    "zstd<1.5.6.8",
    "clickhouse-sqlalchemy>=0.2.0,<0.2.5",
}

dataplex_common = {
    "google-cloud-dataplex",
    # Pinned to 0.2.2 because 0.3.0 changed the import path from
    # google.cloud.datacatalog.lineage_v1 to google.cloud.datacatalog_lineage,
    # which breaks existing code using the old import path
    "google-cloud-datacatalog-lineage==0.2.2",
    "tenacity>=8.0.1",
}

redshift_common = {
    # Clickhouse 0.8.3 adds support for SQLAlchemy 1.4.x
    "sqlalchemy-redshift>=0.8.3",
    "GeoAlchemy2",
    "redshift-connector>=2.1.5",
    *path_spec_common,
}

snowflake_common = {
    # Lower bound due to https://github.com/snowflakedb/snowflake-sqlalchemy/issues/350
    #
    # Upper bound <1.7.4: Version 1.7.4 of snowflake-sqlalchemy introduced a bug that breaks
    # table column name reflection for non-uppercase table names. While we do not
    # use this method directly, it is used by great-expectations during profiling.
    #
    # See: https://github.com/snowflakedb/snowflake-sqlalchemy/compare/v1.7.3...v1.7.4
    #
    # The exact cause of the breakage in v1.7.4 is unclear, but it may be related to
    # changes in the _get_table_columns function. I initially suspected PR #541
    # (https://github.com/snowflakedb/snowflake-sqlalchemy/pull/541), but that has been
    # present since v1.7.0 and does not appear to cause issues.
    #
    # Reflection failures for case-sensitive object names are a known issue:
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/388
    #
    # As of May 2025, snowflake-sqlalchemy is in maintenance mode. I have commented on the
    # above issue and we are pinning to a safe version.
    "snowflake-sqlalchemy>=1.4.3, <1.7.4",
    "snowflake-connector-python>=3.4.0",
    "pandas",
    "cryptography",
    "msal",
    *cachetools_lib,
    *classification_lib,
}

trino = {
    "trino[sqlalchemy]>=0.308",
}

pyhive_common = {
    # DataHub maintains a fork of PyHive
    # - 0.6.11 adds support for table comments and column comments,
    #   and also releases HTTP and HTTPS transport schemes
    # - 0.6.12 adds support for Spark Thrift Server
    # - 0.6.13 adds a small fix for Databricks
    # - 0.6.14 uses pure-sasl instead of sasl so it builds on Python 3.11
    # - 0.6.15 adds support for thrift > 0.14 (cherry-picked from https://github.com/apache/thrift/pull/2491)
    # - 0.6.16 fixes a regression in 0.6.15 (https://github.com/acryldata/PyHive/pull/9)
    "acryl-pyhive[hive-pure-sasl]==0.6.16",
    # As per https://github.com/datahub-project/datahub/issues/8405
    # and https://github.com/dropbox/PyHive/issues/417, version 0.14.0
    # of thrift broke PyHive's hive+http transport.
    # Fixed by https://github.com/apache/thrift/pull/2491 in version 0.17.0
    # which is unfortunately not on PyPi.
    # Instead, we put the fix in our PyHive fork, so no thrift pin is needed.
}

microsoft_common = {
    "msal>=1.31.1",
}

iceberg_common = {
    # PyIceberg dependency restrictions history:
    # - From v0.4.0, pydantic v2 is required.
    # - From v0.8.0, there have been changes to the catalog connection configuration details -
    # especially for AWS-based catalogs and warehouses, the properties `profile_name`, `region_name`,
    # `aws_access_key_id`, `aws_secret_access_key`, and `aws_session_token` were deprecated and removed in version
    # 0.8.0.
    "pyiceberg[glue,hive,dynamodb,snappy,hive,s3fs,adlfs,pyarrow,zstandard]>=0.8.0",
    # Pin pydantic due to incompatibility with pyiceberg 0.9.1.
    # pyiceberg 0.9.1 requires pydantic>=2.0,<2.12
    "pydantic<2.12",
    *cachetools_lib,
}

mssql_common = {
    "sqlalchemy-pytds>=0.3",
    "pyOpenSSL",
}

postgres_common = {
    "psycopg2-binary",
    "GeoAlchemy2",
}

s3_base = {
    *aws_common,
    "more-itertools>=8.12.0",
    "parse>=1.19.0",
    *pyarrow_common,
    "tableschema>=1.20.2",
    # ujson 5.2.0 has the JSONDecodeError exception type, which we need for error handling.
    "ujson>=5.2.0",
    "smart-open[s3]>=5.2.1",
    # moto 5.0.0 drops support for Python 3.7
    "moto[s3]<5.0.0",
    *path_spec_common,
    # cachetools is used by operation_config which is imported by profiling config
    *cachetools_lib,
}

threading_timeout_common = {
    "stopit==1.1.2",
    # stopit uses pkg_resources internally, which means there's an implied
    # dependency on setuptools.
    "setuptools",
}

abs_base = {
    "azure-core>=1.31.0",
    "azure-identity>=1.21.0",
    "azure-storage-blob>=12.19.0",
    "azure-storage-file-datalake>=12.14.0",
    "more-itertools>=8.12.0",
    *pyarrow_common,
    "smart-open[azure]>=5.2.1",
    "tableschema>=1.20.2",
    "ujson>=5.2.0",
    *path_spec_common,
}

azure_data_factory = {
    "azure-identity>=1.21.0",
    "azure-mgmt-datafactory>=9.0.0",
}

data_lake_profiling = {
    "pydeequ>=1.1.0",
    "pyspark~=3.5.6",
    # cachetools is used by the profiling config
    *cachetools_lib,
}

delta_lake = {
    *s3_base,
    *abs_base,
    # Version 0.18.0 broken on ARM Macs: https://github.com/delta-io/delta-rs/issues/2577
    # Version 1.0.2 breaks due to Unsupported reader features required: [DeletionVectors]: https://github.com/delta-io/delta-rs/issues/1094
    "deltalake>=0.6.3, != 0.6.4, != 0.18.0, <1.0.0; platform_system == 'Darwin' and platform_machine == 'arm64'",
    "deltalake>=0.6.3, != 0.6.4, <1.0.0; platform_system != 'Darwin' or platform_machine != 'arm64'",
}

powerbi_report_server = {"requests", "requests_ntlm"}

slack = {
    "slack-sdk==3.18.1",
    "tenacity>=8.0.1",
}

databricks_common = {
    # Version 2.4.0 includes sqlalchemy dialect, 2.8.0 includes some bug fixes
    # Version 3.0.0 required SQLAlchemy > 2.0.21
    # TODO: When upgrading to >=3.0.0, remove proxy authentication monkey patching
    # in src/datahub/ingestion/source/unity/proxy.py (_patch_databricks_sql_proxy_auth)
    # as the fix was included natively in 3.0.0 via https://github.com/databricks/databricks-sql-python/pull/354
    "databricks-sql-connector>=2.8.0,<3.0.0",
}

databricks = {
    # 0.1.11 appears to have authentication issues with azure databricks
    # 0.22.0 has support for `include_browse` in metadata list apis
    "databricks-sdk>=0.30.0",
    "pyspark~=3.5.6",
    "requests",
    # Due to https://github.com/databricks/databricks-sql-python/issues/326
    # databricks-sql-connector<3.0.0 requires pandas<2.2.0
    "pandas<2.2.0",
}

mysql = {"pymysql>=1.0.2"}

sac = {
    "requests",
    "pyodata>=1.11.1",
    "Authlib",
}

superset_common = {
    "requests",
    *sqlglot_lib,
}

# Note: for all of these, framework_common will be added.
plugins: Dict[str, Set[str]] = {
    # Sink plugins.
    "datahub-kafka": {
        # At some moment, we decoupled from using here kafka_common
        # That's becuase kafka_common has more strict lower bound versions that conflict with airflow depedency constraints
        "confluent_kafka[schemaregistry,avro]>=1.9.0, != 2.8.1",
        "fastavro>=1.2.0",
    },
    "datahub-rest": rest_common,
    "sync-file-emitter": {"filelock"},
    "datahub-lite": {
        "duckdb>=1.0.0",
        "fastapi",
        "uvicorn",
    },
    # Integrations.
    "airflow": {
        f"acryl-datahub-airflow-plugin{_self_pin}",
    },
    "circuit-breaker": {
        # In gql v4, the execute() method's signature changed. Since we've updated
        # our code to use the new signature, we need to pin to gql v4.
        "gql[requests]>=4.0.0",
    },
    # TODO: Eventually we should reorganize our imports so that this depends on sqlalchemy_lib
    # but not the full sql_common.
    "datahub": sql_common | mysql | kafka_common,
    "great-expectations": {
        f"acryl-datahub-gx-plugin{_self_pin}",
    },
    # Misc plugins.
    "sql-parser": sqlglot_lib,
    # Source plugins
    # sqlalchemy-bigquery is included here since it provides an implementation of
    # a SQLalchemy-conform STRUCT type definition
    "athena": sql_common
    # We need to set tenacity lower than 8.4.0 as
    # this version has missing dependency asyncio
    # https://github.com/jd/tenacity/issues/471
    | {
        "PyAthena[SQLAlchemy]>=2.6.0,<3.0.0",
        "sqlalchemy-bigquery>=1.4.1",
        "tenacity!=8.4.0",
    },
    "azure-ad": set(),
    "azure-data-factory": azure_data_factory,
    "bigquery": sql_common
    | bigquery_common
    | sqlglot_lib
    | classification_lib
    | {
        # Pinned to 0.2.2 because 0.3.0 changed the import path from
        # google.cloud.datacatalog.lineage_v1 to google.cloud.datacatalog_lineage,
        # which breaks existing code using the old import path
        "google-cloud-datacatalog-lineage==0.2.2",
    },
    "bigquery-slim": bigquery_common,
    "bigquery-queries": sql_common | bigquery_common | sqlglot_lib,
    "clickhouse": sql_common | clickhouse_common,
    "clickhouse-usage": sql_common | usage_common | clickhouse_common,
    "cockroachdb": sql_common
    | postgres_common
    | aws_common
    | {"sqlalchemy-cockroachdb<2.0.0"},
    "datahub-lineage-file": set(),
    "datahub-business-glossary": set(),
    "dataplex": dataplex_common,
    "delta-lake": {*data_lake_profiling, *delta_lake},
    "dbt": {"requests"} | dbt_common | aws_common,
    "dbt-cloud": {"requests"} | dbt_common,
    "dremio": {"requests"} | sql_common,
    "druid": sql_common | {"pydruid>=0.6.2"},
    "dynamodb": aws_common | classification_lib,
    # Starting with 7.14.0 python client is checking if it is connected to elasticsearch client. If its not it throws
    # UnsupportedProductError
    # https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/release-notes.html#rn-7-14-0
    # https://github.com/elastic/elasticsearch-py/issues/1639#issuecomment-883587433
    "elasticsearch": {"elasticsearch==7.13.4", *cachetools_lib},
    "excel": {
        "openpyxl>=3.1.5",
        "pandas",
        *aws_common,
        *abs_base,
        *cachetools_lib,
        *data_lake_profiling,
    },
    "cassandra": {
        "cassandra-driver>=3.28.0",
        # We were seeing an error like this `numpy.dtype size changed, may indicate binary incompatibility. Expected 96 from C header, got 88 from PyObject`
        # with numpy 2.0. This likely indicates a mismatch between scikit-learn and numpy versions.
        # https://stackoverflow.com/questions/40845304/runtimewarning-numpy-dtype-size-changed-may-indicate-binary-incompatibility
        "numpy<2",
        *cachetools_lib,
    },
    "feast": {
        "feast>=0.34.0,<1",
        "flask-openid>=1.3.0",
        "dask[dataframe]<2024.7.0",
        # We were seeing an error like this `numpy.dtype size changed, may indicate binary incompatibility. Expected 96 from C header, got 88 from PyObject`
        # with numpy 2.0. This likely indicates a mismatch between scikit-learn and numpy versions.
        # https://stackoverflow.com/questions/40845304/runtimewarning-numpy-dtype-size-changed-may-indicate-binary-incompatibility
        "numpy<2",
    },
    "grafana": {"requests", *sqlglot_lib},
    "glue": aws_common | cachetools_lib,
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
        *great_expectations_lib,
    },
    # keep in sync with presto-on-hive until presto-on-hive will be removed
    # Supports both SQL (psycopg2/pymysql) and Thrift (pymetastore) connection types
    # kerberos is required for GSSAPI auth (pure-sasl delegates to it)
    "hive-metastore": sql_common
    | pyhive_common
    | {"psycopg2-binary", "pymysql>=1.0.2", "pymetastore>=0.4.2", "tenacity>=8.0.1", "kerberos>=1.3.0"},
    "iceberg": iceberg_common,
    "iceberg-catalog": aws_common,
    "json-schema": {"requests"},
    "kafka": kafka_common | kafka_protobuf,
    "kafka-connect": sql_common | {"requests", "JPype1"},
    "ldap": {"python-ldap>=2.4"},
    "looker": looker_common,
    "lookml": looker_common,
    "metabase": {"requests"} | sqlglot_lib,
    "mlflow": {
        "mlflow-skinny>=2.3.0,<2.21.0",
        # Pinned to avoid the breaking change introduced in MLflow 2.21.0 where search_registered_models injects an implicit filter
        # https://github.com/mlflow/mlflow/pull/14795
        # Upper bound can be removed once the upstream issue is resolved,
        # or we have a reliable and backward-compatible way to handle prompt filtering.
        # It's technically wrong for packages to depend on setuptools. However, it seems mlflow does it anyways.
        "setuptools",
    },
    "datahub-debug": {"dnspython==2.7.0", "requests"},
    "mode": {"requests", "python-liquid", "tenacity>=8.0.1"} | sqlglot_lib,
    "mongodb": {"pymongo>=4.8.0", "packaging"},
    "mssql": sql_common | mssql_common,
    "mssql-odbc": sql_common | mssql_common | {"pyodbc"},
    "mysql": sql_common | mysql | aws_common,
    # mariadb should have same dependency as mysql
    "mariadb": sql_common | mysql | aws_common,
    "okta": {"okta~=1.7.0", "nest-asyncio"},
    "oracle": sql_common | {"oracledb"},
    "postgres": sql_common | postgres_common | aws_common,
    "presto": sql_common | pyhive_common | trino,
    # presto-on-hive is an alias for hive-metastore and needs to be kept in sync
    "presto-on-hive": sql_common
    | pyhive_common
    | {"psycopg2-binary", "pymysql>=1.0.2"},
    "pulsar": {"requests"},
    "redash": {"redash-toolbelt", "sql-metadata"} | sqlglot_lib,
    "redshift": sql_common
    | redshift_common
    | usage_common
    | sqlglot_lib
    | classification_lib
    | {"db-dtypes"}  # Pandas extension data types
    | cachetools_lib,
    # S3 includes PySpark by default for profiling support (backward compatible)
    # Standard installation: pip install 'acryl-datahub[s3]' (with PySpark)
    # Lightweight installation: pip install 'acryl-datahub[s3-slim]' (no PySpark, no profiling)
    "s3": {*s3_base, *data_lake_profiling},
    "s3-slim": {*s3_base},
    "gcs": {*s3_base, *data_lake_profiling, "smart-open[gcs]>=5.2.1"},
    "abs": {*abs_base, *data_lake_profiling},
    "sagemaker": aws_common,
    "salesforce": {"simple-salesforce", *cachetools_lib},
    "snowflake": snowflake_common | sql_common | usage_common | sqlglot_lib,
    "snowflake-slim": snowflake_common,
    "snowflake-summary": snowflake_common | sql_common | usage_common | sqlglot_lib,
    "snowflake-queries": snowflake_common | sql_common | usage_common | sqlglot_lib,
    "sqlalchemy": sql_common,
    "sql-queries": usage_common | sqlglot_lib | aws_common | {"smart-open[s3]>=5.2.1"},
    "slack": slack,
    "superset": superset_common,
    "preset": superset_common,
    "tableau": {"tableauserverclient>=0.24.0"} | sqlglot_lib,
    "teradata": sql_common
    | usage_common
    | sqlglot_lib
    | {
        # On 2024-10-30, teradatasqlalchemy 20.0.0.2 was released. This version seemed to cause issues
        # in our CI, so we're pinning the version for now.
        "teradatasqlalchemy>=17.20.0.0,<=20.0.0.2",
    },
    "trino": sql_common | trino,
    "starburst-trino-usage": sql_common | usage_common | trino,
    "nifi": {"requests", "packaging", "requests-gssapi"},
    "powerbi": (
        microsoft_common
        | {"lark[regex]==1.1.4", "sqlparse", "more-itertools"}
        | sqlglot_lib
        | threading_timeout_common
    ),
    "powerbi-report-server": powerbi_report_server,
    "vertica": sql_common | {"vertica-sqlalchemy-dialect[vertica-python]==0.0.8.2"},
    "unity-catalog": databricks_common | databricks | sql_common,
    # databricks is alias for unity-catalog and needs to be kept in sync
    "databricks": databricks_common | databricks | sql_common,
    "fivetran": snowflake_common
    | bigquery_common
    | databricks_common
    | sqlalchemy_lib
    | sqlglot_lib,
    "snaplogic": set(),
    "qlik-sense": sqlglot_lib | {"requests", "websocket-client"},
    "sigma": sqlglot_lib | {"requests"},
    "sac": sac,
    "neo4j": {"pandas", "neo4j"},
    "vertexai": {"google-cloud-aiplatform>=1.80.0"},
}

# This is mainly used to exclude plugins from the Docker image.
all_exclude_plugins: Set[str] = {
    # The Airflow extra is only retained for compatibility, but new users should
    # be using the datahub-airflow-plugin package instead.
    "airflow",
    # The great-expectations extra is only retained for compatibility, but new users should
    # be using the datahub-gx-plugin package instead.
    "great-expectations",
    # SQL Server ODBC requires additional drivers, and so we don't want to keep
    # it included in the default "all" installation.
    "mssql-odbc",
    # duckdb doesn't have a prebuilt wheel for Linux arm7l or aarch64, so we
    # simply exclude it.
    "datahub-lite",
    # Feast tends to have overly restrictive dependencies and hence doesn't
    # play nice with the "all" installation.
    "feast",
}

mypy_stubs = {
    "types-dataclasses",
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
    "types-cachetools",
    # versions 0.1.13 and 0.1.14 seem to have issues
    "types-click==0.1.12",
    # The boto3-stubs package seems to have regularly breaking minor releases,
    # we pin to a specific version to avoid this.
    "boto3-stubs[s3,glue,sagemaker,sts,dynamodb, lakeformation]==1.40.0",
    "types-tabulate",
    # avrogen package requires this
    "types-pytz",
    "types-pyOpenSSL",
    "types-click-spinner>=0.1.13.1",
    "types-ujson>=5.2.0",
    "types-Deprecated",
    "types-protobuf>=4.21.0.1",
    "sqlalchemy2-stubs",
}


test_api_requirements = {
    "pytest>=6.2.2",
    "pytest-timeout",
    # Missing numpy requirement in 8.0.0
    "deepdiff!=8.0.0",
    "orderly-set!=5.4.0",  # 5.4.0 uses invalid types on older Python versions
    "PyYAML",
    "pytest-docker>=1.1.0",
}

debug_requirements = {
    "memray",
}

lint_requirements = {
    # This is pinned only to avoid spurious errors in CI.
    # We should make an effort to keep it up to date.
    "ruff==0.11.7",
    "mypy==1.17.1",
}

base_dev_requirements = {
    *base_requirements,
    *framework_common,
    *mypy_stubs,
    *s3_base,
    *lint_requirements,
    *test_api_requirements,
    "coverage>=5.1",
    "faker>=18.4.0",
    "pytest-asyncio>=0.16.0",
    "pytest-cov>=2.8.1",
    "pytest-random-order~=1.1.0",
    "requests-mock",
    "freezegun",  # TODO: fully remove and use time-machine
    "time-machine",  # better Pydantic v2 compatibility
    "jsonpickle",
    "build",
    "twine",
    *list(
        dependency
        for plugin in [
            "abs",
            "athena",
            "bigquery",
            "clickhouse",
            "clickhouse-usage",
            "cockroachdb",
            "dataplex",
            "delta-lake",
            "dremio",
            "druid",
            "elasticsearch",
            "feast",
            "iceberg",
            "iceberg-catalog",
            "mlflow",
            "mongodb",
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
            "presto",
            "redash",
            "redshift",
            "s3",
            "snowflake",
            "snaplogic",
            "slack",
            "tableau",
            "teradata",
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
            "fivetran",
            "kafka-connect",
            "qlik-sense",
            "sigma",
            "sac",
            "cassandra",
            "neo4j",
            "vertexai",
            "mssql-odbc",
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
            "azure-data-factory",
            "circuit-breaker",
            "clickhouse",
            "delta-lake",
            "druid",
            "excel",
            "feast",
            "hana",
            "hive",
            "hive-metastore",
            "iceberg",
            "iceberg-catalog",
            "kafka-connect",
            "ldap",
            "slack",
            "mssql",
            "mssql-odbc",
            "mysql",
            "mariadb",
            "redash",
            "vertica",
            "vertexai",
        ]
        if plugin
        for dependency in plugins[plugin]
    ),
}

entry_points = {
    "console_scripts": ["datahub = datahub.entrypoints:main"],
    "datahub.ingestion.source.plugins": [
        "abs = datahub.ingestion.source.abs.source:ABSSource",
        "csv-enricher = datahub.ingestion.source.csv_enricher:CSVEnricherSource",
        "file = datahub.ingestion.source.file:GenericFileSource",
        "datahub = datahub.ingestion.source.datahub.datahub_source:DataHubSource",
        "sqlalchemy = datahub.ingestion.source.sql.sql_generic:SQLAlchemyGenericSource",
        "athena = datahub.ingestion.source.sql.athena:AthenaSource",
        "azure-ad = datahub.ingestion.source.identity.azure_ad:AzureADSource",
        "azure-data-factory = datahub.ingestion.source.azure_data_factory.adf_source:AzureDataFactorySource",
        "bigquery = datahub.ingestion.source.bigquery_v2.bigquery:BigqueryV2Source",
        "bigquery-queries = datahub.ingestion.source.bigquery_v2.bigquery_queries:BigQueryQueriesSource",
        "clickhouse = datahub.ingestion.source.sql.clickhouse:ClickHouseSource",
        "clickhouse-usage = datahub.ingestion.source.usage.clickhouse_usage:ClickHouseUsageSource",
        "cockroachdb = datahub.ingestion.source.sql.cockroachdb:CockroachDBSource",
        "delta-lake = datahub.ingestion.source.delta_lake:DeltaLakeSource",
        "s3 = datahub.ingestion.source.s3:S3Source",
        "dbt = datahub.ingestion.source.dbt.dbt_core:DBTCoreSource",
        "dbt-cloud = datahub.ingestion.source.dbt.dbt_cloud:DBTCloudSource",
        "dremio = datahub.ingestion.source.dremio.dremio_source:DremioSource",
        "druid = datahub.ingestion.source.sql.druid:DruidSource",
        "dynamodb = datahub.ingestion.source.dynamodb.dynamodb:DynamoDBSource",
        "elasticsearch = datahub.ingestion.source.elastic_search:ElasticsearchSource",
        "excel = datahub.ingestion.source.excel.source:ExcelSource",
        "feast = datahub.ingestion.source.feast:FeastRepositorySource",
        "grafana = datahub.ingestion.source.grafana.grafana_source:GrafanaSource",
        "glue = datahub.ingestion.source.aws.glue:GlueSource",
        "sagemaker = datahub.ingestion.source.aws.sagemaker:SagemakerSource",
        "hana = datahub.ingestion.source.sql.hana:HanaSource",
        "hive = datahub.ingestion.source.sql.hive.hive_source:HiveSource",
        "hive-metastore = datahub.ingestion.source.sql.hive.hive_metastore_source:HiveMetastoreSource",
        "json-schema = datahub.ingestion.source.schema.json_schema:JsonSchemaSource",
        "kafka = datahub.ingestion.source.kafka.kafka:KafkaSource",
        "kafka-connect = datahub.ingestion.source.kafka_connect.kafka_connect:KafkaConnectSource",
        "ldap = datahub.ingestion.source.ldap:LDAPSource",
        "looker = datahub.ingestion.source.looker.looker_source:LookerDashboardSource",
        "lookml = datahub.ingestion.source.looker.lookml_source:LookMLSource",
        "datahub-gc = datahub.ingestion.source.gc.datahub_gc:DataHubGcSource",
        "datahub-debug = datahub.ingestion.source.debug.datahub_debug:DataHubDebugSource",
        "datahub-apply = datahub.ingestion.source.apply.datahub_apply:DataHubApplySource",
        "datahub-mock-data = datahub.ingestion.source.mock_data.datahub_mock_data:DataHubMockDataSource",
        "datahub-lineage-file = datahub.ingestion.source.metadata.lineage:LineageFileSource",
        "datahub-business-glossary = datahub.ingestion.source.metadata.business_glossary:BusinessGlossaryFileSource",
        "dataplex = datahub.ingestion.source.dataplex.dataplex:DataplexSource",
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
        "slack = datahub.ingestion.source.slack.slack:SlackSource",
        "snowflake = datahub.ingestion.source.snowflake.snowflake_v2:SnowflakeV2Source",
        "snowflake-summary = datahub.ingestion.source.snowflake.snowflake_summary:SnowflakeSummarySource",
        "snowflake-queries = datahub.ingestion.source.snowflake.snowflake_queries:SnowflakeQueriesSource",
        "superset = datahub.ingestion.source.superset:SupersetSource",
        "preset = datahub.ingestion.source.preset:PresetSource",
        "tableau = datahub.ingestion.source.tableau.tableau:TableauSource",
        "openapi = datahub.ingestion.source.openapi:OpenApiSource",
        "metabase = datahub.ingestion.source.metabase:MetabaseSource",
        "teradata = datahub.ingestion.source.sql.teradata:TeradataSource",
        "trino = datahub.ingestion.source.sql.trino:TrinoSource",
        "starburst-trino-usage = datahub.ingestion.source.usage.starburst_trino_usage:TrinoUsageSource",
        "nifi = datahub.ingestion.source.nifi:NifiSource",
        "powerbi = datahub.ingestion.source.powerbi.powerbi:PowerBiDashboardSource",
        "powerbi-report-server = datahub.ingestion.source.powerbi_report_server:PowerBiReportServerDashboardSource",
        "iceberg = datahub.ingestion.source.iceberg.iceberg:IcebergSource",
        "vertica = datahub.ingestion.source.sql.vertica:VerticaSource",
        "presto = datahub.ingestion.source.sql.presto:PrestoSource",
        # This is only here for backward compatibility. Use the `hive-metastore` source instead.
        "presto-on-hive = datahub.ingestion.source.sql.hive.hive_metastore_source:HiveMetastoreSource",
        "pulsar = datahub.ingestion.source.pulsar:PulsarSource",
        "salesforce = datahub.ingestion.source.salesforce:SalesforceSource",
        "demo-data = datahub.ingestion.source.demo_data.DemoDataSource",
        "unity-catalog = datahub.ingestion.source.unity.source:UnityCatalogSource",
        "gcs = datahub.ingestion.source.gcs.gcs_source:GCSSource",
        "sql-queries = datahub.ingestion.source.sql_queries:SqlQueriesSource",
        "fivetran = datahub.ingestion.source.fivetran.fivetran:FivetranSource",
        "snaplogic = datahub.ingestion.source.snaplogic.snaplogic:SnaplogicSource",
        "qlik-sense = datahub.ingestion.source.qlik_sense.qlik_sense:QlikSenseSource",
        "sigma = datahub.ingestion.source.sigma.sigma:SigmaSource",
        "sac = datahub.ingestion.source.sac.sac:SACSource",
        "cassandra = datahub.ingestion.source.cassandra.cassandra:CassandraSource",
        "neo4j = datahub.ingestion.source.neo4j.neo4j_source:Neo4jSource",
        "vertexai = datahub.ingestion.source.vertexai.vertexai:VertexAISource",
        "hex = datahub.ingestion.source.hex.hex:HexSource",
    ],
    "datahub.ingestion.transformer.plugins": [
        "pattern_cleanup_ownership = datahub.ingestion.transformer.pattern_cleanup_ownership:PatternCleanUpOwnership",
        "simple_remove_dataset_ownership = datahub.ingestion.transformer.remove_dataset_ownership:SimpleRemoveDatasetOwnership",
        "mark_dataset_status = datahub.ingestion.transformer.mark_dataset_status:MarkDatasetStatus",
        "set_dataset_browse_path = datahub.ingestion.transformer.add_dataset_browse_path:AddDatasetBrowsePathTransformer",
        "set_browse_path = datahub.ingestion.transformer.set_browse_path:SetBrowsePathTransformer",
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
        "extract_ownership_from_tags = datahub.ingestion.transformer.extract_ownership_from_tags:ExtractOwnersFromTagsTransformer",
        "add_dataset_dataproduct = datahub.ingestion.transformer.add_dataset_dataproduct:AddDatasetDataProduct",
        "simple_add_dataset_dataproduct = datahub.ingestion.transformer.add_dataset_dataproduct:SimpleAddDatasetDataProduct",
        "pattern_add_dataset_dataproduct = datahub.ingestion.transformer.add_dataset_dataproduct:PatternAddDatasetDataProduct",
        "replace_external_url = datahub.ingestion.transformer.replace_external_url:ReplaceExternalUrlDataset",
        "replace_external_url_container = datahub.ingestion.transformer.replace_external_url:ReplaceExternalUrlContainer",
        "pattern_cleanup_dataset_usage_user = datahub.ingestion.transformer.pattern_cleanup_dataset_usage_user:PatternCleanupDatasetUsageUser",
        "domain_mapping_based_on_tags = datahub.ingestion.transformer.dataset_domain_based_on_tags:DatasetTagDomainMapper",
        "tags_to_term = datahub.ingestion.transformer.tags_to_terms:TagsToTermMapper",
        "tags_to_structured_properties = datahub.ingestion.transformer.tags_to_structured_properties:TagsToStructuredPropertiesTransformer",
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
        "file = datahub.ingestion.source.state_provider.file_ingestion_checkpointing_provider:FileIngestionCheckpointingProvider",
    ],
    "datahub.ingestion.reporting_provider.plugins": [
        "datahub = datahub.ingestion.reporting.datahub_ingestion_run_summary_provider:DatahubIngestionRunSummaryProvider",
        "file = datahub.ingestion.reporting.file_reporter:FileReporter",
    ],
    "datahub.custom_packages": [],
    "datahub.fs.plugins": [
        "s3 = datahub.ingestion.fs.s3_fs:S3FileSystem",
        "file = datahub.ingestion.fs.local_fs:LocalFileSystem",
        "http = datahub.ingestion.fs.http_fs:HttpFileSystem",
    ],
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=_version,
    url="https://docs.datahub.com/",
    project_urls={
        "Documentation": "https://docs.datahub.com/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
        "Releases": "https://github.com/acryldata/datahub/releases",
    },
    license="Apache-2.0",
    description="A CLI to work with DataHub metadata",
    long_description="""\
The `acryl-datahub` package contains a CLI and SDK for interacting with DataHub,
as well as an integration framework for pulling/pushing metadata from external systems.

See the [DataHub docs](https://docs.datahub.com/docs/metadata-ingestion).
""",
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ],
    # Package info.
    zip_safe=False,
    python_requires=">=3.9",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    package_data={
        "datahub": ["py.typed"],
        "datahub.metadata": ["schema.avsc"],
        "datahub.metadata.schemas": ["*.avsc"],
        "datahub.ingestion.source.powerbi": ["powerbi-lexical-grammar.rule"],
        "datahub.ingestion.autogenerated": ["*.json"],
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
        "lint": list(lint_requirements),
        "testing-utils": list(test_api_requirements),  # To import `datahub.testing`
        "integration-tests": list(full_test_dev_requirements),
        "debug": list(debug_requirements),
    },
)
