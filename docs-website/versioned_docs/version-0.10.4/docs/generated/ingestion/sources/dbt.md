---
sidebar_position: 8
title: dbt
slug: /generated/ingestion/sources/dbt
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/dbt.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# dbt

There are 2 sources that provide integration with dbt

<table>
<tr><td>Source Module</td><td>Documentation</td></tr><tr>
<td>

`dbt`

</td>
<td>

The artifacts used by this source are:

- [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - This file contains model, source, tests and lineage data.
- [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
  - This file contains schema data.
  - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
- [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
  - This file contains metadata for sources with freshness checks.
  - We transfer dbt's freshness checks to DataHub's last-modified fields.
  - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.
- [dbt run_results file](https://docs.getdbt.com/reference/artifacts/run-results-json)
  - This file contains metadata from the result of a dbt run, e.g. dbt test
  - When provided, we transfer dbt test run results into assertion run events to see a timeline of test runs on the dataset
    [Read more...](#module-dbt)

</td>
</tr>
<tr>
<td>

`dbt-cloud`

</td>
<td>

This source pulls dbt metadata directly from the dbt Cloud APIs.

You'll need to have a dbt Cloud job set up to run your dbt project, and "Generate docs on run" should be enabled.

The token should have the "read metadata" permission.

To get the required IDs, go to the job details page (this is the one with the "Run History" table), and look at the URL.
It should look something like this: https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094.
In this example, the account ID is 107298, the project ID is 175705, and the job ID is 148094.
[Read more...](#module-dbt-cloud)

</td>
</tr>
</table>

Ingesting metadata from dbt requires either using the **dbt** module or the **dbt-cloud** module.

### Concept Mapping

| Source Concept  | DataHub Concept                                               | Notes              |
| --------------- | ------------------------------------------------------------- | ------------------ |
| `"dbt"`         | [Data Platform](../../metamodel/entities/dataPlatform.md)     |                    |
| dbt Source      | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `source`   |
| dbt Seed        | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `seed`     |
| dbt Model       | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `model`    |
| dbt Snapshot    | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `snapshot` |
| dbt Test        | [Assertion](../../metamodel/entities/assertion.md)            |                    |
| dbt Test Result | [Assertion Run Result](../../metamodel/entities/assertion.md) |                    |

Note:

1. It also generates lineage between the `dbt` nodes (e.g. ephemeral nodes that depend on other dbt sources) as well as lineage between the `dbt` nodes and the underlying (target) platform nodes (e.g. BigQuery Table -> dbt Source, dbt View -> BigQuery View).
2. We also support automated actions (like add a tag, term or owner) based on properties defined in dbt meta.

## Module `dbt`

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                          |
| ---------------------------------------------------------------------------------------------------------- | ------ | ------------------------------ |
| Dataset Usage                                                                                              | ❌     |                                |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Enabled via stateful ingestion |
| Table-Level Lineage                                                                                        | ✅     | Enabled by default             |

The artifacts used by this source are:

- [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - This file contains model, source, tests and lineage data.
- [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
  - This file contains schema data.
  - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
- [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
  - This file contains metadata for sources with freshness checks.
  - We transfer dbt's freshness checks to DataHub's last-modified fields.
  - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.
- [dbt run_results file](https://docs.getdbt.com/reference/artifacts/run-results-json)
  - This file contains metadata from the result of a dbt run, e.g. dbt test
  - When provided, we transfer dbt test run results into assertion run events to see a timeline of test runs on the dataset

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[dbt]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "dbt"
  config:
    # Coordinates
    # To use this as-is, set the environment variable DBT_PROJECT_ROOT to the root folder of your dbt project
    manifest_path: "${DBT_PROJECT_ROOT}/target/manifest_file.json"
    catalog_path: "${DBT_PROJECT_ROOT}/target/catalog_file.json"
    sources_path: "${DBT_PROJECT_ROOT}/target/sources_file.json" # optional for freshness
    test_results_path: "${DBT_PROJECT_ROOT}/target/run_results.json" # optional for recording dbt test results after running dbt test

    # Options
    target_platform: "my_target_platform_id" # e.g. bigquery/postgres/etc.

# sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                             | Description                                                                                                                                                                                                                                                                                                                                                                                 |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| <div className="path-line"><span className="path-main">catalog_path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json Note this can be a local file or a URI.                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">manifest_path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                           | Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json Note this can be a local file or a URI.                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">target_platform</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                         | The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">column_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                                                           | mapping rules that will be executed against dbt column meta properties. Refer to the section below on dbt meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                  |
| <div className="path-line"><span className="path-main">convert_column_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                             | When enabled, converts column URNs to lowercase to ensure cross-platform compatibility. If `target_platform` is Snowflake, the default is True. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                            |
| <div className="path-line"><span className="path-main">enable_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                          | When enabled, applies the mappings that are defined through the meta_mapping directives. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">enable_owner_extraction</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                      | When enabled, ownership info will be extracted from the dbt meta <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">enable_query_tag_mapping</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                     | When enabled, applies the mappings that are defined through the `query_tag_mapping` directives. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">include_env_in_assertion_guid</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                | Prior to version 0.9.4.2, the assertion GUIDs did not include the environment. If you're using multiple dbt ingestion that are only distinguished by env, then you should set this flag to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                           |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                          | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                            |
| <div className="path-line"><span className="path-main">meta_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                                                                  | mapping rules that will be executed against dbt meta properties. Refer to the section below on dbt meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                         |
| <div className="path-line"><span className="path-main">owner_extraction_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                      | Regex string to extract owner from the dbt node using the `(?P<name>...) syntax` of the [match object](https://docs.python.org/3/library/re.html#match-objects), where the group name must be `owner`. Examples: (1)`r"(?P<owner>(.*)): (\w+) (\w+)"` will extract `jdoe` as the owner from `"jdoe: John Doe"` (2) `r"@(?P<owner>(.*))"` will extract `alice` as the owner from `"@alice"`. |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                             | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">query_tag_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                                                             | mapping rules that will be executed against dbt query_tag meta properties. Refer to the section below on dbt meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                               |
| <div className="path-line"><span className="path-main">sources_path</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                  | Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. If not specified, last-modified fields will not be populated. Note this can be a local file or a URI.                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">sql_parser_use_external_process</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                              | When enabled, sql parser will run in isolated in a separate process. This can affect processing time but can protect from sql parser's mem leak. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                           |
| <div className="path-line"><span className="path-main">strip_user_ids_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                    | Whether or not to strip email id while adding owners using dbt meta actions. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">tag_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                    | Prefix added to tags during ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">dbt:</span></div>                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">target_platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                      | The platform instance for the platform that dbt is operating on. Use this if you have multiple instances of the same platform (e.g. redshift) and need to distinguish between them.                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">test_results_path</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                             | Path to output of dbt test run as run_results file in JSON format. See https://docs.getdbt.com/reference/artifacts/run-results-json. If not specified, test execution results will not be populated in DataHub.                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">use_identifiers</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                              | Use model identifier instead of model name if defined (if not, default to model name). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">write_semantics</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                               | Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE" <div className="default-line default-line-with-docs">Default: <span className="default-value">PATCH</span></div>                                                                                                    |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                           | Environment to use in namespace when constructing URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">aws_connection</span></div> <div className="type-name-line"><span className="type-name">AwsConnectionConfig</span></div>                                                                                                   | When fetching manifest files from s3, configuration for aws connection details                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_region</span>&nbsp;<abbr title="Required if aws_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | AWS region code.                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                         | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                               | Named AWS profile to use. Only used if access key / secret are unset. If not set the default will be used                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                        |                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                     | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                         | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, union(anyOf), string, AwsAssumeRoleConfig</span></div>                | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are documented at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role                                   |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if aws_role is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | External ID to use when assuming the role.                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">entities_enabled</span></div> <div className="type-name-line"><span className="type-name">DBTEntitiesEnabled</span></div>                                                                                                  | Controls for enabling / disabling metadata emission for different dbt entities (models, test definitions, test results, etc.) <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;models&#x27;: &#x27;YES&#x27;, &#x27;sources&#x27;: &#x27;YES&#x27;, &#x27;seeds&#x27;: &#x27;YES&#x27;...</span></div>                             |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">models</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                    | Emit metadata for dbt models when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">seeds</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                     | Emit metadata for dbt seeds when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">snapshots</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                 | Emit metadata for dbt snapshots when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">sources</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                   | Emit metadata for dbt sources when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">test_definitions</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                          | Emit metadata for test definitions when enabled when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">test_results</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                              | Emit metadata for test results when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">git_info</span></div> <div className="type-name-line"><span className="type-name">GitReference</span></div>                                                                                                                | Reference to your git location to enable easy navigation from DataHub to your dbt files.                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo</span>&nbsp;<abbr title="Required if git_info is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                   | Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                          | Branch on which your files live by default. Typically main or master. This can also be a commit hash. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div>                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_template</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                    | Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}                                                                                                                                         |
| <div className="path-line"><span className="path-main">node_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                   | regex patterns for dbt model names to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                  |
| <div className="path-line"><span className="path-prefix">node_name_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                           |                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">node_name_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                            |                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">node_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                            | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                                | DBT Stateful Ingestion Config.                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                              | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "DBTCoreConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "incremental_lineage": {
      "title": "Incremental Lineage",
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "default": true,
      "type": "boolean"
    },
    "sql_parser_use_external_process": {
      "title": "Sql Parser Use External Process",
      "description": "When enabled, sql parser will run in isolated in a separate process. This can affect processing time but can protect from sql parser's mem leak.",
      "default": false,
      "type": "boolean"
    },
    "env": {
      "title": "Env",
      "description": "Environment to use in namespace when constructing URNs.",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "stateful_ingestion": {
      "title": "Stateful Ingestion",
      "description": "DBT Stateful Ingestion Config.",
      "allOf": [
        {
          "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
        }
      ]
    },
    "target_platform": {
      "title": "Target Platform",
      "description": "The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)",
      "type": "string"
    },
    "target_platform_instance": {
      "title": "Target Platform Instance",
      "description": "The platform instance for the platform that dbt is operating on. Use this if you have multiple instances of the same platform (e.g. redshift) and need to distinguish between them.",
      "type": "string"
    },
    "use_identifiers": {
      "title": "Use Identifiers",
      "description": "Use model identifier instead of model name if defined (if not, default to model name).",
      "default": false,
      "type": "boolean"
    },
    "entities_enabled": {
      "title": "Entities Enabled",
      "description": "Controls for enabling / disabling metadata emission for different dbt entities (models, test definitions, test results, etc.)",
      "default": {
        "models": "YES",
        "sources": "YES",
        "seeds": "YES",
        "snapshots": "YES",
        "test_definitions": "YES",
        "test_results": "YES"
      },
      "allOf": [
        {
          "$ref": "#/definitions/DBTEntitiesEnabled"
        }
      ]
    },
    "tag_prefix": {
      "title": "Tag Prefix",
      "description": "Prefix added to tags during ingestion.",
      "default": "dbt:",
      "type": "string"
    },
    "node_name_pattern": {
      "title": "Node Name Pattern",
      "description": "regex patterns for dbt model names to filter in ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "meta_mapping": {
      "title": "Meta Mapping",
      "description": "mapping rules that will be executed against dbt meta properties. Refer to the section below on dbt meta automated mappings.",
      "default": {},
      "type": "object"
    },
    "column_meta_mapping": {
      "title": "Column Meta Mapping",
      "description": "mapping rules that will be executed against dbt column meta properties. Refer to the section below on dbt meta automated mappings.",
      "default": {},
      "type": "object"
    },
    "query_tag_mapping": {
      "title": "Query Tag Mapping",
      "description": "mapping rules that will be executed against dbt query_tag meta properties. Refer to the section below on dbt meta automated mappings.",
      "default": {},
      "type": "object"
    },
    "write_semantics": {
      "title": "Write Semantics",
      "description": "Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be \"PATCH\" or \"OVERRIDE\"",
      "default": "PATCH",
      "type": "string"
    },
    "strip_user_ids_from_email": {
      "title": "Strip User Ids From Email",
      "description": "Whether or not to strip email id while adding owners using dbt meta actions.",
      "default": false,
      "type": "boolean"
    },
    "enable_owner_extraction": {
      "title": "Enable Owner Extraction",
      "description": "When enabled, ownership info will be extracted from the dbt meta",
      "default": true,
      "type": "boolean"
    },
    "owner_extraction_pattern": {
      "title": "Owner Extraction Pattern",
      "description": "Regex string to extract owner from the dbt node using the `(?P<name>...) syntax` of the [match object](https://docs.python.org/3/library/re.html#match-objects), where the group name must be `owner`. Examples: (1)`r\"(?P<owner>(.*)): (\\w+) (\\w+)\"` will extract `jdoe` as the owner from `\"jdoe: John Doe\"` (2) `r\"@(?P<owner>(.*))\"` will extract `alice` as the owner from `\"@alice\"`.",
      "type": "string"
    },
    "include_env_in_assertion_guid": {
      "title": "Include Env In Assertion Guid",
      "description": "Prior to version 0.9.4.2, the assertion GUIDs did not include the environment. If you're using multiple dbt ingestion that are only distinguished by env, then you should set this flag to True.",
      "default": false,
      "type": "boolean"
    },
    "convert_column_urns_to_lowercase": {
      "title": "Convert Column Urns To Lowercase",
      "description": "When enabled, converts column URNs to lowercase to ensure cross-platform compatibility. If `target_platform` is Snowflake, the default is True.",
      "default": false,
      "type": "boolean"
    },
    "enable_meta_mapping": {
      "title": "Enable Meta Mapping",
      "description": "When enabled, applies the mappings that are defined through the meta_mapping directives.",
      "default": true,
      "type": "boolean"
    },
    "enable_query_tag_mapping": {
      "title": "Enable Query Tag Mapping",
      "description": "When enabled, applies the mappings that are defined through the `query_tag_mapping` directives.",
      "default": true,
      "type": "boolean"
    },
    "manifest_path": {
      "title": "Manifest Path",
      "description": "Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json Note this can be a local file or a URI.",
      "type": "string"
    },
    "catalog_path": {
      "title": "Catalog Path",
      "description": "Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json Note this can be a local file or a URI.",
      "type": "string"
    },
    "sources_path": {
      "title": "Sources Path",
      "description": "Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. If not specified, last-modified fields will not be populated. Note this can be a local file or a URI.",
      "type": "string"
    },
    "test_results_path": {
      "title": "Test Results Path",
      "description": "Path to output of dbt test run as run_results file in JSON format. See https://docs.getdbt.com/reference/artifacts/run-results-json. If not specified, test execution results will not be populated in DataHub.",
      "type": "string"
    },
    "aws_connection": {
      "title": "Aws Connection",
      "description": "When fetching manifest files from s3, configuration for aws connection details",
      "allOf": [
        {
          "$ref": "#/definitions/AwsConnectionConfig"
        }
      ]
    },
    "git_info": {
      "title": "Git Info",
      "description": "Reference to your git location to enable easy navigation from DataHub to your dbt files.",
      "allOf": [
        {
          "$ref": "#/definitions/GitReference"
        }
      ]
    }
  },
  "required": [
    "target_platform",
    "manifest_path",
    "catalog_path"
  ],
  "additionalProperties": false,
  "definitions": {
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "EmitDirective": {
      "title": "EmitDirective",
      "description": "A holder for directives for emission for specific types of entities",
      "enum": [
        "YES",
        "NO",
        "ONLY"
      ]
    },
    "DBTEntitiesEnabled": {
      "title": "DBTEntitiesEnabled",
      "description": "Controls which dbt entities are going to be emitted by this source",
      "type": "object",
      "properties": {
        "models": {
          "description": "Emit metadata for dbt models when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "sources": {
          "description": "Emit metadata for dbt sources when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "seeds": {
          "description": "Emit metadata for dbt seeds when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "snapshots": {
          "description": "Emit metadata for dbt snapshots when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "test_definitions": {
          "description": "Emit metadata for test definitions when enabled when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "test_results": {
          "description": "Emit metadata for test results when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        }
      },
      "additionalProperties": false
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "AwsAssumeRoleConfig": {
      "title": "AwsAssumeRoleConfig",
      "type": "object",
      "properties": {
        "RoleArn": {
          "title": "Rolearn",
          "description": "ARN of the role to assume.",
          "type": "string"
        },
        "ExternalId": {
          "title": "Externalid",
          "description": "External ID to use when assuming the role.",
          "type": "string"
        }
      },
      "required": [
        "RoleArn"
      ]
    },
    "AwsConnectionConfig": {
      "title": "AwsConnectionConfig",
      "description": "Common AWS credentials config.\n\nCurrently used by:\n    - Glue source\n    - SageMaker source\n    - dbt source",
      "type": "object",
      "properties": {
        "aws_access_key_id": {
          "title": "Aws Access Key Id",
          "description": "AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "type": "string"
        },
        "aws_secret_access_key": {
          "title": "Aws Secret Access Key",
          "description": "AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "type": "string"
        },
        "aws_session_token": {
          "title": "Aws Session Token",
          "description": "AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "type": "string"
        },
        "aws_role": {
          "title": "Aws Role",
          "description": "AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are documented at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role",
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "array",
              "items": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "$ref": "#/definitions/AwsAssumeRoleConfig"
                  }
                ]
              }
            }
          ]
        },
        "aws_profile": {
          "title": "Aws Profile",
          "description": "Named AWS profile to use. Only used if access key / secret are unset. If not set the default will be used",
          "type": "string"
        },
        "aws_region": {
          "title": "Aws Region",
          "description": "AWS region code.",
          "type": "string"
        },
        "aws_endpoint_url": {
          "title": "Aws Endpoint Url",
          "description": "The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
          "type": "string"
        },
        "aws_proxy": {
          "title": "Aws Proxy",
          "description": "A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        }
      },
      "required": [
        "aws_region"
      ],
      "additionalProperties": false
    },
    "GitReference": {
      "title": "GitReference",
      "description": "Reference to a hosted Git repository. Used to generate \"view source\" links.",
      "type": "object",
      "properties": {
        "repo": {
          "title": "Repo",
          "description": "Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.",
          "type": "string"
        },
        "branch": {
          "title": "Branch",
          "description": "Branch on which your files live by default. Typically main or master. This can also be a commit hash.",
          "default": "main",
          "type": "string"
        },
        "url_template": {
          "title": "Url Template",
          "description": "Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}",
          "type": "string"
        }
      },
      "required": [
        "repo"
      ],
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

### dbt meta automated mappings

dbt allows authors to define meta properties for datasets. Checkout this link to know more - [dbt meta](https://docs.getdbt.com/reference/resource-configs/meta). Our dbt source allows users to define
actions such as add a tag, term or owner. For example if a dbt model has a meta config `"has_pii": True`, we can define an action
that evaluates if the property is set to true and add, lets say, a `pii` tag.
To leverage this feature we require users to define mappings as part of the recipe. The following section describes how you can build these mappings. Listed below is a `meta_mapping` and `column_meta_mapping` section that among other things, looks for keys like `business_owner` and adds owners that are listed there.

```yaml
meta_mapping:
  business_owner:
    match: ".*"
    operation: "add_owner"
    config:
      owner_type: user
      owner_category: BUSINESS_OWNER
  has_pii:
    match: True
    operation: "add_tag"
    config:
      tag: "has_pii_test"
  int_property:
    match: 1
    operation: "add_tag"
    config:
      tag: "int_meta_property"
  double_property:
    match: 2.5
    operation: "add_term"
    config:
      term: "double_meta_property"
  data_governance.team_owner:
    match: "Finance"
    operation: "add_term"
    config:
      term: "Finance_test"
  terms_list:
    match: ".*"
    operation: "add_terms"
    config:
      separator: ","
column_meta_mapping:
  terms_list:
    match: ".*"
    operation: "add_terms"
    config:
      separator: ","
  is_sensitive:
    match: True
    operation: "add_tag"
    config:
      tag: "sensitive"
```

We support the following operations:

1. add_tag - Requires `tag` property in config.
2. add_term - Requires `term` property in config.
3. add_terms - Accepts an optional `separator` property in config.
4. add_owner - Requires `owner_type` property in config which can be either user or group. Optionally accepts the `owner_category` config property which you can set to one of `['TECHNICAL_OWNER', 'BUSINESS_OWNER', 'DATA_STEWARD', 'DATAOWNER'` (defaults to `DATAOWNER`).

Note:

1. The dbt `meta_mapping` config works at the model level, while the `column_meta_mapping` config works at the column level. The `add_owner` operation is not supported at the column level.
2. For string meta properties we support regex matching.

With regex matching, you can also use the matched value to customize how you populate the tag, term or owner fields. Here are a few advanced examples:

#### Data Tier - Bronze, Silver, Gold

If your meta section looks like this:

```yaml
meta:
  data_tier: Bronze # chosen from [Bronze,Gold,Silver]
```

and you wanted to attach a glossary term like `urn:li:glossaryTerm:Bronze` for all the models that have this value in the meta section attached to them, the following meta_mapping section would achieve that outcome:

```yaml
meta_mapping:
  data_tier:
    match: "Bronze|Silver|Gold"
    operation: "add_term"
    config:
      term: "{{ $match }}"
```

to match any data_tier of Bronze, Silver or Gold and maps it to a glossary term with the same name.

#### Case Numbers - create tags

If your meta section looks like this:

```yaml
meta:
  case: PLT-4678 # internal Case Number
```

and you want to generate tags that look like `case_4678` from this, you can use the following meta_mapping section:

```yaml
meta_mapping:
  case:
    match: "PLT-(.*)"
    operation: "add_tag"
     config:
       tag: "case_{{ $match }}"
```

#### Stripping out leading @ sign

You can also match specific groups within the value to extract subsets of the matched value. e.g. if you have a meta section that looks like this:

```yaml
meta:
  owner: "@finance-team"
  business_owner: "@janet"
```

and you want to mark the finance-team as a group that owns the dataset (skipping the leading @ sign), while marking janet as an individual user (again, skipping the leading @ sign) that owns the dataset, you can use the following meta-mapping section.

```yaml
meta_mapping:
  owner:
    match: "^@(.*)"
    operation: "add_owner"
    config:
      owner_type: group
  business_owner:
    match: "^@(?P<owner>(.*))"
    operation: "add_owner"
    config:
      owner_type: user
      owner_category: BUSINESS_OWNER
```

In the examples above, we show two ways of writing the matching regexes. In the first one, `^@(.*)` the first matching group (a.k.a. match.group(1)) is automatically inferred. In the second example, `^@(?P<owner>(.*))`, we use a named matching group (called owner, since we are matching an owner) to capture the string we want to provide to the ownership urn.

### dbt query_tag automated mappings

This works similarly as the dbt meta mapping but for the query tags

We support the below actions -

1. add_tag - Requires `tag` property in config.

The below example set as global tag the query tag `tag` key's value.

```json
"query_tag_mapping":
{
   "tag":
      "match": ".*"
      "operation": "add_tag"
      "config":
        "tag": "{{ $match }}"
}
```

### Integrating with dbt test

To integrate with dbt tests, the `dbt` source needs access to the `run_results.json` file generated after a `dbt test` execution. Typically, this is written to the `target` directory. A common pattern you can follow is:

1. Run `dbt docs generate` and upload `manifest.json` and `catalog.json` to a location accessible to the `dbt` source (e.g. s3 or local file system)
2. Run `dbt test` and upload `run_results.json` to a location accessible to the `dbt` source (e.g. s3 or local file system)
3. Run `datahub ingest -c dbt_recipe.dhub.yaml` with the following config parameters specified
   - test_results_path: pointing to the run_results.json file that you just created

The connector will produce the following things:

- Assertion definitions that are attached to the dataset (or datasets)
- Results from running the tests attached to the timeline of the dataset

#### View of dbt tests for a dataset

![test view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-tests-view.png)

#### Viewing the SQL for a dbt test

![test logic view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-test-logic-view.png)

#### Viewing timeline for a failed dbt test

![test view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-tests-failure-view.png)

#### Separating test result emission from other metadata emission

You can segregate emission of test results from the emission of other dbt metadata using the `entities_enabled` config flag.
The following recipe shows you how to emit only test results.

```yaml
source:
  type: dbt
  config:
    manifest_path: _path_to_manifest_json
    catalog_path: _path_to_catalog_json
    test_results_path: _path_to_run_results_json
    target_platform: postgres
    entities_enabled:
      test_results: Only
```

Similarly, the following recipe shows you how to emit everything (i.e. models, sources, seeds, test definitions) but not test results:

```yaml
source:
  type: dbt
  config:
    manifest_path: _path_to_manifest_json
    catalog_path: _path_to_catalog_json
    run_results_path: _path_to_run_results_json
    target_platform: postgres
    entities_enabled:
      test_results: No
```

### Code Coordinates

- Class Name: `datahub.ingestion.source.dbt.dbt_core.DBTCoreSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dbt/dbt_core.py)

## Module `dbt-cloud`

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                          |
| ---------------------------------------------------------------------------------------------------------- | ------ | ------------------------------ |
| Dataset Usage                                                                                              | ❌     |                                |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Enabled via stateful ingestion |
| Table-Level Lineage                                                                                        | ✅     | Enabled by default             |

This source pulls dbt metadata directly from the dbt Cloud APIs.

You'll need to have a dbt Cloud job set up to run your dbt project, and "Generate docs on run" should be enabled.

The token should have the "read metadata" permission.

To get the required IDs, go to the job details page (this is the one with the "Run History" table), and look at the URL.
It should look something like this: https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094.
In this example, the account ID is 107298, the project ID is 175705, and the job ID is 148094.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[dbt-cloud]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "dbt-cloud"
  config:
    token: ${DBT_CLOUD_TOKEN}

    # In the URL https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094,
    # 107298 is the account_id, 175705 is the project_id, and 148094 is the job_id

    account_id: # set to your dbt cloud account id
    project_id: # set to your dbt cloud project id
    job_id: # set to your dbt cloud job id
    run_id: # set to your dbt cloud run id. This is optional, and defaults to the latest run

    target_platform: postgres

    # Options
    target_platform: "my_target_platform_id" # e.g. bigquery/postgres/etc.

# sink configs

```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                              | Description                                                                                                                                                                                                                                                                                                                                                                                 |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| <div className="path-line"><span className="path-main">account_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div>                              | The DBT Cloud account ID to use.                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">job_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                  | The ID of the job to ingest metadata from.                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">project_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div>                              | The dbt Cloud project ID to use.                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">target_platform</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                          | The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                    | The API token to use to authenticate with DBT Cloud.                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">column_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                            | mapping rules that will be executed against dbt column meta properties. Refer to the section below on dbt meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                  |
| <div className="path-line"><span className="path-main">convert_column_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                              | When enabled, converts column URNs to lowercase to ensure cross-platform compatibility. If `target_platform` is Snowflake, the default is True. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                            |
| <div className="path-line"><span className="path-main">enable_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                           | When enabled, applies the mappings that are defined through the meta_mapping directives. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">enable_owner_extraction</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | When enabled, ownership info will be extracted from the dbt meta <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">enable_query_tag_mapping</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                      | When enabled, applies the mappings that are defined through the `query_tag_mapping` directives. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">include_env_in_assertion_guid</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                 | Prior to version 0.9.4.2, the assertion GUIDs did not include the environment. If you're using multiple dbt ingestion that are only distinguished by env, then you should set this flag to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                           |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                           | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                            |
| <div className="path-line"><span className="path-main">meta_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                   | mapping rules that will be executed against dbt meta properties. Refer to the section below on dbt meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                         |
| <div className="path-line"><span className="path-main">metadata_endpoint</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | The dbt Cloud metadata API endpoint. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://metadata.cloud.getdbt.com/graphql</span></div>                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">owner_extraction_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | Regex string to extract owner from the dbt node using the `(?P<name>...) syntax` of the [match object](https://docs.python.org/3/library/re.html#match-objects), where the group name must be `owner`. Examples: (1)`r"(?P<owner>(.*)): (\w+) (\w+)"` will extract `jdoe` as the owner from `"jdoe: John Doe"` (2) `r"@(?P<owner>(.*))"` will extract `alice` as the owner from `"@alice"`. |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">query_tag_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                              | mapping rules that will be executed against dbt query_tag meta properties. Refer to the section below on dbt meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                               |
| <div className="path-line"><span className="path-main">run_id</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                        | The ID of the run to ingest metadata from. If not specified, we'll default to the latest run.                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">sql_parser_use_external_process</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                               | When enabled, sql parser will run in isolated in a separate process. This can affect processing time but can protect from sql parser's mem leak. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                           |
| <div className="path-line"><span className="path-main">strip_user_ids_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                     | Whether or not to strip email id while adding owners using dbt meta actions. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">tag_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                     | Prefix added to tags during ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">dbt:</span></div>                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">target_platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | The platform instance for the platform that dbt is operating on. Use this if you have multiple instances of the same platform (e.g. redshift) and need to distinguish between them.                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">use_identifiers</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                               | Use model identifier instead of model name if defined (if not, default to model name). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">write_semantics</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                | Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE" <div className="default-line default-line-with-docs">Default: <span className="default-value">PATCH</span></div>                                                                                                    |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | Environment to use in namespace when constructing URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">entities_enabled</span></div> <div className="type-name-line"><span className="type-name">DBTEntitiesEnabled</span></div>                                                   | Controls for enabling / disabling metadata emission for different dbt entities (models, test definitions, test results, etc.) <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;models&#x27;: &#x27;YES&#x27;, &#x27;sources&#x27;: &#x27;YES&#x27;, &#x27;seeds&#x27;: &#x27;YES&#x27;...</span></div>                             |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">models</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                     | Emit metadata for dbt models when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">seeds</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                      | Emit metadata for dbt seeds when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">snapshots</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                  | Emit metadata for dbt snapshots when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">sources</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                    | Emit metadata for dbt sources when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">test_definitions</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>           | Emit metadata for test definitions when enabled when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">entities_enabled.</span><span className="path-main">test_results</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>               | Emit metadata for test results when set to Yes or Only <div className="default-line default-line-with-docs">Default: <span className="default-value">YES</span></div>                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">node_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                    | regex patterns for dbt model names to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                  |
| <div className="path-line"><span className="path-prefix">node_name_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>            |                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">node_name_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>             |                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">node_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>             | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                 | DBT Stateful Ingestion Config.                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "DBTCloudConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "incremental_lineage": {
      "title": "Incremental Lineage",
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "default": true,
      "type": "boolean"
    },
    "sql_parser_use_external_process": {
      "title": "Sql Parser Use External Process",
      "description": "When enabled, sql parser will run in isolated in a separate process. This can affect processing time but can protect from sql parser's mem leak.",
      "default": false,
      "type": "boolean"
    },
    "env": {
      "title": "Env",
      "description": "Environment to use in namespace when constructing URNs.",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "stateful_ingestion": {
      "title": "Stateful Ingestion",
      "description": "DBT Stateful Ingestion Config.",
      "allOf": [
        {
          "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
        }
      ]
    },
    "target_platform": {
      "title": "Target Platform",
      "description": "The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)",
      "type": "string"
    },
    "target_platform_instance": {
      "title": "Target Platform Instance",
      "description": "The platform instance for the platform that dbt is operating on. Use this if you have multiple instances of the same platform (e.g. redshift) and need to distinguish between them.",
      "type": "string"
    },
    "use_identifiers": {
      "title": "Use Identifiers",
      "description": "Use model identifier instead of model name if defined (if not, default to model name).",
      "default": false,
      "type": "boolean"
    },
    "entities_enabled": {
      "title": "Entities Enabled",
      "description": "Controls for enabling / disabling metadata emission for different dbt entities (models, test definitions, test results, etc.)",
      "default": {
        "models": "YES",
        "sources": "YES",
        "seeds": "YES",
        "snapshots": "YES",
        "test_definitions": "YES",
        "test_results": "YES"
      },
      "allOf": [
        {
          "$ref": "#/definitions/DBTEntitiesEnabled"
        }
      ]
    },
    "tag_prefix": {
      "title": "Tag Prefix",
      "description": "Prefix added to tags during ingestion.",
      "default": "dbt:",
      "type": "string"
    },
    "node_name_pattern": {
      "title": "Node Name Pattern",
      "description": "regex patterns for dbt model names to filter in ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "meta_mapping": {
      "title": "Meta Mapping",
      "description": "mapping rules that will be executed against dbt meta properties. Refer to the section below on dbt meta automated mappings.",
      "default": {},
      "type": "object"
    },
    "column_meta_mapping": {
      "title": "Column Meta Mapping",
      "description": "mapping rules that will be executed against dbt column meta properties. Refer to the section below on dbt meta automated mappings.",
      "default": {},
      "type": "object"
    },
    "query_tag_mapping": {
      "title": "Query Tag Mapping",
      "description": "mapping rules that will be executed against dbt query_tag meta properties. Refer to the section below on dbt meta automated mappings.",
      "default": {},
      "type": "object"
    },
    "write_semantics": {
      "title": "Write Semantics",
      "description": "Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be \"PATCH\" or \"OVERRIDE\"",
      "default": "PATCH",
      "type": "string"
    },
    "strip_user_ids_from_email": {
      "title": "Strip User Ids From Email",
      "description": "Whether or not to strip email id while adding owners using dbt meta actions.",
      "default": false,
      "type": "boolean"
    },
    "enable_owner_extraction": {
      "title": "Enable Owner Extraction",
      "description": "When enabled, ownership info will be extracted from the dbt meta",
      "default": true,
      "type": "boolean"
    },
    "owner_extraction_pattern": {
      "title": "Owner Extraction Pattern",
      "description": "Regex string to extract owner from the dbt node using the `(?P<name>...) syntax` of the [match object](https://docs.python.org/3/library/re.html#match-objects), where the group name must be `owner`. Examples: (1)`r\"(?P<owner>(.*)): (\\w+) (\\w+)\"` will extract `jdoe` as the owner from `\"jdoe: John Doe\"` (2) `r\"@(?P<owner>(.*))\"` will extract `alice` as the owner from `\"@alice\"`.",
      "type": "string"
    },
    "include_env_in_assertion_guid": {
      "title": "Include Env In Assertion Guid",
      "description": "Prior to version 0.9.4.2, the assertion GUIDs did not include the environment. If you're using multiple dbt ingestion that are only distinguished by env, then you should set this flag to True.",
      "default": false,
      "type": "boolean"
    },
    "convert_column_urns_to_lowercase": {
      "title": "Convert Column Urns To Lowercase",
      "description": "When enabled, converts column URNs to lowercase to ensure cross-platform compatibility. If `target_platform` is Snowflake, the default is True.",
      "default": false,
      "type": "boolean"
    },
    "enable_meta_mapping": {
      "title": "Enable Meta Mapping",
      "description": "When enabled, applies the mappings that are defined through the meta_mapping directives.",
      "default": true,
      "type": "boolean"
    },
    "enable_query_tag_mapping": {
      "title": "Enable Query Tag Mapping",
      "description": "When enabled, applies the mappings that are defined through the `query_tag_mapping` directives.",
      "default": true,
      "type": "boolean"
    },
    "metadata_endpoint": {
      "title": "Metadata Endpoint",
      "description": "The dbt Cloud metadata API endpoint.",
      "default": "https://metadata.cloud.getdbt.com/graphql",
      "type": "string"
    },
    "token": {
      "title": "Token",
      "description": "The API token to use to authenticate with DBT Cloud.",
      "type": "string"
    },
    "account_id": {
      "title": "Account Id",
      "description": "The DBT Cloud account ID to use.",
      "type": "integer"
    },
    "project_id": {
      "title": "Project Id",
      "description": "The dbt Cloud project ID to use.",
      "type": "integer"
    },
    "job_id": {
      "title": "Job Id",
      "description": "The ID of the job to ingest metadata from.",
      "type": "integer"
    },
    "run_id": {
      "title": "Run Id",
      "description": "The ID of the run to ingest metadata from. If not specified, we'll default to the latest run.",
      "type": "integer"
    }
  },
  "required": [
    "target_platform",
    "token",
    "account_id",
    "project_id",
    "job_id"
  ],
  "additionalProperties": false,
  "definitions": {
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "EmitDirective": {
      "title": "EmitDirective",
      "description": "A holder for directives for emission for specific types of entities",
      "enum": [
        "YES",
        "NO",
        "ONLY"
      ]
    },
    "DBTEntitiesEnabled": {
      "title": "DBTEntitiesEnabled",
      "description": "Controls which dbt entities are going to be emitted by this source",
      "type": "object",
      "properties": {
        "models": {
          "description": "Emit metadata for dbt models when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "sources": {
          "description": "Emit metadata for dbt sources when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "seeds": {
          "description": "Emit metadata for dbt seeds when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "snapshots": {
          "description": "Emit metadata for dbt snapshots when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "test_definitions": {
          "description": "Emit metadata for test definitions when enabled when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        },
        "test_results": {
          "description": "Emit metadata for test results when set to Yes or Only",
          "default": "YES",
          "allOf": [
            {
              "$ref": "#/definitions/EmitDirective"
            }
          ]
        }
      },
      "additionalProperties": false
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.dbt.dbt_cloud.DBTCloudSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dbt/dbt_cloud.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for dbt, feel free to ping us on [our Slack](https://slack.datahubproject.io).
