---
sidebar_position: 25
title: Fivetran
slug: /generated/ingestion/sources/fivetran
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/fivetran.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Fivetran
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ✅ | Enabled by default, can be disabled via configuration `include_column_lineage`. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |


This plugin extracts fivetran users, connectors, destinations and sync history.
This plugin is in beta and has only been tested on Snowflake connector.

## Integration Details

This source extracts the following:

- Connectors in fivetran as Data Pipelines and Data Jobs to represent data lineage information between source and destination.
- Connector sources - DataJob input Datasets.
- Connector destination - DataJob output Datasets.
- Connector runs - DataProcessInstances as DataJob runs.

## Configuration Notes

1. Fivetran supports the fivetran platform connector to dump the log events and connectors, destinations, users and roles metadata in your destination.
2. You need to setup and start the initial sync of the fivetran platform connector before using this source. Refer [link](https://fivetran.com/docs/logs/fivetran-platform/setup-guide).
3. Once initial sync up of your fivetran platform connector is done, you need to provide the fivetran platform connector's destination platform and its configuration in the recipe.

## Concept mapping

| Fivetran        | Datahub                                                                                               |
| --------------- | ----------------------------------------------------------------------------------------------------- |
| `Connector`     | [DataJob](/docs/generated/metamodel/entities/datajob/)                        |
| `Source`        | [Dataset](/docs/generated/metamodel/entities/dataset/)                        |
| `Destination`   | [Dataset](/docs/generated/metamodel/entities/dataset/)                        |
| `Connector Run` | [DataProcessInstance](/docs/generated/metamodel/entities/dataprocessinstance) |

Source and destination are mapped to Dataset as an Input and Output of Connector.

## Current limitations

Works only for

- Snowflake destination
- Bigquery destination

## Snowflake destination Configuration Guide

1. If your fivetran platform connector destination is snowflake, you need to provide user details and its role with correct privileges in order to fetch metadata.
2. Snowflake system admin can follow this guide to create a fivetran_datahub role, assign it the required privileges, and assign it to a user by executing the following Snowflake commands from a user with the ACCOUNTADMIN role or MANAGE GRANTS privilege.

```sql
create or replace role fivetran_datahub;

// Grant access to a warehouse to run queries to view metadata
grant operate, usage on warehouse "<your-warehouse>" to role fivetran_datahub;

// Grant access to view database and schema in which your log and metadata tables exist
grant usage on DATABASE "<fivetran-log-database>" to role fivetran_datahub;
grant usage on SCHEMA "<fivetran-log-database>"."<fivetran-log-schema>" to role fivetran_datahub;

// Grant access to execute select query on schema in which your log and metadata tables exist
grant select on all tables in SCHEMA "<fivetran-log-database>"."<fivetran-log-schema>" to role fivetran_datahub;

// Grant the fivetran_datahub to the snowflake user.
grant role fivetran_datahub to user snowflake_user;
```

## Bigquery destination Configuration Guide

1. If your fivetran platform connector destination is bigquery, you need to setup a ServiceAccount as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and select BigQuery Data Viewer and BigQuery Job User IAM roles.
2. Create and Download a service account JSON keyfile and provide bigquery connection credential in bigquery destination config.

## Advanced Configurations

### Working with Platform Instances

If you have multiple instances of source/destination systems that are referred in your `fivetran` setup, you'd need to configure platform instance for these systems in `fivetran` recipe to generate correct lineage edges. Refer the document [Working with Platform Instances](/docs/platform-instances) to understand more about this.

While configuring the platform instance for source system you need to provide connector id as key and for destination system provide destination id as key.
When creating the connection details in the fivetran UI make a note of the destination Group ID of the service account, as that will need to be used in the `destination_to_platform_instance` configuration.
I.e:

<p align="center">
  <img width="70%"  src="https://github.com/datahub-project/static-assets/raw/main/imgs/integrations/bigquery/bq-connection-id.png"/>
</p>

In this case the configuration would be something like:

```yaml
destination_to_platform_instance:
  greyish_positive: <--- this comes from bigquery destination - see screenshot
    database: <big query project ID>
    env: PROD
```

#### Example - Multiple Postgres Source Connectors each reading from different postgres instance

```yml
# Map of connector source to platform instance
sources_to_platform_instance:
  postgres_connector_id1:
    platform_instance: cloud_postgres_instance
    env: PROD

  postgres_connector_id2:
    platform_instance: local_postgres_instance
    env: DEV
```

#### Example - Multiple Snowflake Destinations each writing to different snowflake instance

```yml
# Map of destination to platform instance
destination_to_platform_instance:
  snowflake_destination_id1:
    platform_instance: prod_snowflake_instance
    env: PROD

  snowflake_destination_id2:
    platform_instance: dev_snowflake_instance
    env: PROD
```

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: fivetran
  config:
    # Fivetran log connector destination server configurations
    fivetran_log_config:
      destination_platform: snowflake
      # Optional - If destination platform is 'snowflake', provide snowflake configuration.
      snowflake_destination_config:
        # Coordinates
        account_id: "abc48144"
        warehouse: "COMPUTE_WH"
        database: "MY_SNOWFLAKE_DB"
        log_schema: "FIVETRAN_LOG"

        # Credentials
        username: "${SNOWFLAKE_USER}"
        password: "${SNOWFLAKE_PASS}"
        role: "snowflake_role"
      # Optional - If destination platform is 'bigquery', provide bigquery configuration.
      bigquery_destination_config:
        # Credentials
        credential:
          private_key_id: "project_key_id"
          project_id: "project_id"
          client_email: "client_email"
          client_id: "client_id"
          private_key: "private_key"
        dataset: "fivetran_log_dataset"

    # Optional - filter for certain connector names instead of ingesting everything.
    # connector_patterns:
    #   allow:
    #     - connector_name

    # Optional -- A mapping of the connector's all sources to its database.
    # sources_to_database:
    #   connector_id: source_db

    # Optional -- This mapping is optional and only required to configure platform-instance for source
    # A mapping of Fivetran connector id to data platform instance
    # sources_to_platform_instance:
    #   connector_id:
    #     platform_instance: cloud_instance
    #     env: DEV

    # Optional -- This mapping is optional and only required to configure platform-instance for destination.
    # A mapping of Fivetran destination id to data platform instance
    # destination_to_platform_instance:
    #   destination_id:
    #     platform_instance: cloud_instance
    #     env: DEV

sink:
  # sink configs

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">fivetran_log_config</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">FivetranLogConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.</span><span className="path-main">destination_platform</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "snowflake", "bigquery" <div className="default-line default-line-with-docs">Default: <span className="default-value">snowflake</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.</span><span className="path-main">bigquery_destination_config</span></div> <div className="type-name-line"><span className="type-name">One of BigQueryDestinationConfig, null</span></div> | If destination platform is 'bigquery', provide bigquery configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.</span><span className="path-main">dataset</span>&nbsp;<abbr title="Required if bigquery_destination_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The fivetran connector log dataset.  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.</span><span className="path-main">extra_client_options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Additional options to pass to google.cloud.logging_v2.client.Client. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.</span><span className="path-main">project_on_behalf</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | [Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.</span><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of GCPCredential, null</span></div> | BigQuery credential informations <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">client_email</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client email  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client Id  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">private_key</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n'  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">private_key_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Private key id  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">auth_provider_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Auth provider x509 certificate url <div className="default-line default-line-with-docs">Default: <span className="default-value">https://www.googleapis.com/oauth2/v1/certs</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">auth_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://accounts.google.com/o/oauth2/auth</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">client_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Project id to set the credentials <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">token_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Token uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://oauth2.googleapis.com/token</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.bigquery_destination_config.credential.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication type <div className="default-line default-line-with-docs">Default: <span className="default-value">service&#95;account</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.</span><span className="path-main">snowflake_destination_config</span></div> <div className="type-name-line"><span className="type-name">One of SnowflakeDestinationConfig, null</span></div> | If destination platform is 'snowflake', provide snowflake configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">account_id</span>&nbsp;<abbr title="Required if snowflake_destination_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Snowflake account identifier. e.g. xy12345,  xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure, xy12345.us-west-2.privatelink. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details.  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">database</span>&nbsp;<abbr title="Required if snowflake_destination_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The fivetran connector log database.  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">log_schema</span>&nbsp;<abbr title="Required if snowflake_destination_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The fivetran connector log schema.  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">authentication_type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of authenticator to use when connecting to Snowflake. Supports "DEFAULT_AUTHENTICATOR", "OAUTH_AUTHENTICATOR", "EXTERNAL_BROWSER_AUTHENTICATOR" and "KEY_PAIR_AUTHENTICATOR". <div className="default-line default-line-with-docs">Default: <span className="default-value">DEFAULT&#95;AUTHENTICATOR</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">connect_args</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | Connect args to pass to Snowflake SqlAlchemy driver <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Snowflake password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">private_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\nencrypted-private-key\n-----END ENCRYPTED PRIVATE KEY-----\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">private_key_password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Password for your private key. Required if using key pair authentication with encrypted private key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">private_key_path</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">role</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Snowflake role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">snowflake_domain</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Snowflake domain. Use 'snowflakecomputing.com' for most regions or 'snowflakecomputing.cn' for China (cn-northwest-1) region. <div className="default-line default-line-with-docs">Default: <span className="default-value">snowflakecomputing.com</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | OAuth token from external identity provider. Not recommended for most use cases because it will not be able to refresh once expired. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Snowflake username. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">warehouse</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Snowflake warehouse. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.</span><span className="path-main">oauth_config</span></div> <div className="type-name-line"><span className="type-name">One of OAuthConfiguration, null</span></div> | oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">authority_url</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authority url of your identity provider  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | client id of your registered application  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">provider</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "microsoft", "okta"  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">scopes</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">array</span></div> | scopes required to connect to snowflake  |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.scopes.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | client secret of the application if use_certificate = false <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">encoded_oauth_private_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | base64 encoded private key content if use_certificate = true <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">encoded_oauth_public_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | base64 encoded certificate content if use_certificate = true <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">fivetran_log_config.snowflake_destination_config.oauth_config.</span><span className="path-main">use_certificate</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Do you want to use certificate and private key to authenticate using oauth <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">history_sync_lookback_period</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The number of days to look back when extracting connectors' sync history. <div className="default-line default-line-with-docs">Default: <span className="default-value">7</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates table->table column lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connector_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">connector_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">connector_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">destination_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">destination_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">destination_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">destination_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">destination_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">destination_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">destination_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override the platform type detection. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The database that all assets produced by this connector belong to. For destinations, this defaults to the fivetran log config's database. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">include_schema_in_urn</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include schema in the dataset URN. In some cases, the schema is not relevant to the dataset URN and Fivetran sets it to the source and destination table names in the connector. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by DataHub platform ingestion source belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">sources_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override the platform type detection. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The database that all assets produced by this connector belong to. For destinations, this defaults to the fivetran log config's database. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">include_schema_in_urn</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include schema in the dataset URN. In some cases, the schema is not relevant to the dataset URN and Fivetran sets it to the source and destination table names in the connector. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by DataHub platform ingestion source belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Fivetran Stateful Ingestion Config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "BigQueryDestinationConfig": {
      "additionalProperties": false,
      "properties": {
        "credential": {
          "anyOf": [
            {
              "$ref": "#/$defs/GCPCredential"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "BigQuery credential informations"
        },
        "extra_client_options": {
          "additionalProperties": true,
          "default": {},
          "description": "Additional options to pass to google.cloud.logging_v2.client.Client.",
          "title": "Extra Client Options",
          "type": "object"
        },
        "project_on_behalf": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account.",
          "title": "Project On Behalf"
        },
        "dataset": {
          "description": "The fivetran connector log dataset.",
          "title": "Dataset",
          "type": "string"
        }
      },
      "required": [
        "dataset"
      ],
      "title": "BigQueryDestinationConfig",
      "type": "object"
    },
    "FivetranLogConfig": {
      "additionalProperties": false,
      "properties": {
        "destination_platform": {
          "default": "snowflake",
          "description": "The destination platform where fivetran connector log tables are dumped.",
          "enum": [
            "snowflake",
            "bigquery"
          ],
          "title": "Destination Platform",
          "type": "string"
        },
        "snowflake_destination_config": {
          "anyOf": [
            {
              "$ref": "#/$defs/SnowflakeDestinationConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If destination platform is 'snowflake', provide snowflake configuration."
        },
        "bigquery_destination_config": {
          "anyOf": [
            {
              "$ref": "#/$defs/BigQueryDestinationConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If destination platform is 'bigquery', provide bigquery configuration."
        }
      },
      "title": "FivetranLogConfig",
      "type": "object"
    },
    "GCPCredential": {
      "additionalProperties": false,
      "properties": {
        "project_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Project id to set the credentials",
          "title": "Project Id"
        },
        "private_key_id": {
          "description": "Private key id",
          "title": "Private Key Id",
          "type": "string"
        },
        "private_key": {
          "description": "Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'",
          "title": "Private Key",
          "type": "string"
        },
        "client_email": {
          "description": "Client email",
          "title": "Client Email",
          "type": "string"
        },
        "client_id": {
          "description": "Client Id",
          "title": "Client Id",
          "type": "string"
        },
        "auth_uri": {
          "default": "https://accounts.google.com/o/oauth2/auth",
          "description": "Authentication uri",
          "title": "Auth Uri",
          "type": "string"
        },
        "token_uri": {
          "default": "https://oauth2.googleapis.com/token",
          "description": "Token uri",
          "title": "Token Uri",
          "type": "string"
        },
        "auth_provider_x509_cert_url": {
          "default": "https://www.googleapis.com/oauth2/v1/certs",
          "description": "Auth provider x509 certificate url",
          "title": "Auth Provider X509 Cert Url",
          "type": "string"
        },
        "type": {
          "default": "service_account",
          "description": "Authentication type",
          "title": "Type",
          "type": "string"
        },
        "client_x509_cert_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
          "title": "Client X509 Cert Url"
        }
      },
      "required": [
        "private_key_id",
        "private_key",
        "client_email",
        "client_id"
      ],
      "title": "GCPCredential",
      "type": "object"
    },
    "OAuthConfiguration": {
      "additionalProperties": false,
      "properties": {
        "provider": {
          "$ref": "#/$defs/OAuthIdentityProvider",
          "description": "Identity provider for oauth.Supported providers are microsoft and okta."
        },
        "authority_url": {
          "description": "Authority url of your identity provider",
          "title": "Authority Url",
          "type": "string"
        },
        "client_id": {
          "description": "client id of your registered application",
          "title": "Client Id",
          "type": "string"
        },
        "scopes": {
          "description": "scopes required to connect to snowflake",
          "items": {
            "type": "string"
          },
          "title": "Scopes",
          "type": "array"
        },
        "use_certificate": {
          "default": false,
          "description": "Do you want to use certificate and private key to authenticate using oauth",
          "title": "Use Certificate",
          "type": "boolean"
        },
        "client_secret": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "client secret of the application if use_certificate = false",
          "title": "Client Secret"
        },
        "encoded_oauth_public_key": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "base64 encoded certificate content if use_certificate = true",
          "title": "Encoded Oauth Public Key"
        },
        "encoded_oauth_private_key": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "base64 encoded private key content if use_certificate = true",
          "title": "Encoded Oauth Private Key"
        }
      },
      "required": [
        "provider",
        "authority_url",
        "client_id",
        "scopes"
      ],
      "title": "OAuthConfiguration",
      "type": "object"
    },
    "OAuthIdentityProvider": {
      "enum": [
        "microsoft",
        "okta"
      ],
      "title": "OAuthIdentityProvider",
      "type": "string"
    },
    "PlatformDetail": {
      "additionalProperties": false,
      "properties": {
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Override the platform type detection.",
          "title": "Platform"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The instance of the platform that all assets produced by this recipe belong to",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by DataHub platform ingestion source belong to",
          "title": "Env",
          "type": "string"
        },
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The database that all assets produced by this connector belong to. For destinations, this defaults to the fivetran log config's database.",
          "title": "Database"
        },
        "include_schema_in_urn": {
          "default": true,
          "description": "Include schema in the dataset URN. In some cases, the schema is not relevant to the dataset URN and Fivetran sets it to the source and destination table names in the connector.",
          "title": "Include Schema In Urn",
          "type": "boolean"
        }
      },
      "title": "PlatformDetail",
      "type": "object"
    },
    "SnowflakeDestinationConfig": {
      "additionalProperties": false,
      "properties": {
        "options": {
          "additionalProperties": true,
          "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
          "title": "Options",
          "type": "object"
        },
        "username": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Snowflake username.",
          "title": "Username"
        },
        "password": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Snowflake password.",
          "title": "Password"
        },
        "private_key": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\\nencrypted-private-key\\n-----END ENCRYPTED PRIVATE KEY-----\\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
          "title": "Private Key"
        },
        "private_key_path": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
          "title": "Private Key Path"
        },
        "private_key_password": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Password for your private key. Required if using key pair authentication with encrypted private key.",
          "title": "Private Key Password"
        },
        "oauth_config": {
          "anyOf": [
            {
              "$ref": "#/$defs/OAuthConfiguration"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth"
        },
        "authentication_type": {
          "default": "DEFAULT_AUTHENTICATOR",
          "description": "The type of authenticator to use when connecting to Snowflake. Supports \"DEFAULT_AUTHENTICATOR\", \"OAUTH_AUTHENTICATOR\", \"EXTERNAL_BROWSER_AUTHENTICATOR\" and \"KEY_PAIR_AUTHENTICATOR\".",
          "title": "Authentication Type",
          "type": "string"
        },
        "account_id": {
          "description": "Snowflake account identifier. e.g. xy12345,  xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure, xy12345.us-west-2.privatelink. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details.",
          "title": "Account Id",
          "type": "string"
        },
        "warehouse": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Snowflake warehouse.",
          "title": "Warehouse"
        },
        "role": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Snowflake role.",
          "title": "Role"
        },
        "connect_args": {
          "anyOf": [
            {
              "additionalProperties": true,
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Connect args to pass to Snowflake SqlAlchemy driver",
          "title": "Connect Args"
        },
        "token": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "OAuth token from external identity provider. Not recommended for most use cases because it will not be able to refresh once expired.",
          "title": "Token"
        },
        "snowflake_domain": {
          "default": "snowflakecomputing.com",
          "description": "Snowflake domain. Use 'snowflakecomputing.com' for most regions or 'snowflakecomputing.cn' for China (cn-northwest-1) region.",
          "title": "Snowflake Domain",
          "type": "string"
        },
        "database": {
          "description": "The fivetran connector log database.",
          "title": "Database",
          "type": "string"
        },
        "log_schema": {
          "description": "The fivetran connector log schema.",
          "title": "Log Schema",
          "type": "string"
        }
      },
      "required": [
        "account_id",
        "database",
        "log_schema"
      ],
      "title": "SnowflakeDestinationConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Fivetran Stateful Ingestion Config."
    },
    "fivetran_log_config": {
      "$ref": "#/$defs/FivetranLogConfig",
      "description": "Fivetran log connector destination server configurations."
    },
    "connector_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Filtering regex patterns for connector names."
    },
    "destination_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for destination ids to filter in ingestion. Fivetran destination IDs are usually two word identifiers e.g. canyon_tolerable, and are not the same as the destination database name. They're visible in the Fivetran UI under Destinations -> Overview -> Destination Group ID."
    },
    "include_column_lineage": {
      "default": true,
      "description": "Populates table->table column lineage.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "sources_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "default": {},
      "description": "A mapping from connector id to its platform/instance/env/database details.",
      "title": "Sources To Platform Instance",
      "type": "object"
    },
    "destination_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "default": {},
      "description": "A mapping of destination id to its platform/instance/env details.",
      "title": "Destination To Platform Instance",
      "type": "object"
    },
    "history_sync_lookback_period": {
      "default": 7,
      "description": "The number of days to look back when extracting connectors' sync history.",
      "title": "History Sync Lookback Period",
      "type": "integer"
    }
  },
  "required": [
    "fivetran_log_config"
  ],
  "title": "FivetranSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.fivetran.fivetran.FivetranSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/fivetran/fivetran.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Fivetran, feel free to ping us on [our Slack](https://datahub.com/slack).
