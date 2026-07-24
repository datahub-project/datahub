


# Google Cloud Storage

## Overview

Google Cloud Storage is a storage and lakehouse platform. Learn more in the [official Google Cloud Storage documentation](https://cloud.google.com/storage).

The DataHub integration for Google Cloud Storage covers file/lakehouse metadata entities such as datasets, paths, and containers. It also captures stateful deletion detection.

## Concept Mapping

| Source Concept                             | DataHub Concept                                                                           | Notes                |
| ------------------------------------------ | ----------------------------------------------------------------------------------------- | -------------------- |
| `"Google Cloud Storage"`                   | [Data Platform](/docs/generated/metamodel/entities/dataplatform/) |                      |
| GCS object / Folder containing GCS objects | [Dataset](/docs/generated/metamodel/entities/dataset/)            |                      |
| GCS bucket                                 | [Container](/docs/generated/metamodel/entities/container/)        | Subtype `GCS bucket` |
| GCS folder                                 | [Container](/docs/generated/metamodel/entities/container/)        | Subtype `Folder`     |


## Module `gcs`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - GCS bucket, Folder. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Schema Metadata | ✅ | Enabled by default. |

### Overview

The `gcs` module ingests metadata from Gcs into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This connector ingests Google Cloud Storage datasets into DataHub. It allows mapping an individual file or a folder of files to a dataset in DataHub.
To specify the group of files that form a dataset, use `path_specs` configuration in ingestion recipe. This source leverages [Interoperability of GCS with S3](https://cloud.google.com/storage/docs/interoperability)
and uses DataHub S3 Data Lake integration source under the hood. Refer section [Path Specs](/docs/generated/ingestion/sources/s3/#path-specs) from S3 connector for more details.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

The GCS source supports three authentication methods:

1. **HMAC Keys** (`hmac`, default): Long-lived key-based authentication using Google Cloud Storage HMAC keys. Suitable for simple setups and service accounts outside GCP.
2. **GKE Workload Identity** (`workload_identity`): Keyless authentication using [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). The recommended option when DataHub runs inside GKE with Workload Identity enabled — no credential config required.
3. **Workload Identity Federation** (`workload_identity_federation`): Keyless, token-based authentication for workloads running _outside_ GCP (AWS, Azure, on-premises). Exchanges an external identity token for short-lived GCP credentials via GCP's STS endpoint.

#### HMAC Authentication

1. Create a service account with "Storage Object Viewer" role — [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create).
2. Ensure you meet the [requirements to generate an HMAC key](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#before-you-begin).
3. Create an HMAC key for the service account — [Create HMAC keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).

#### GKE Workload Identity

This is the simplest option for DataHub running inside GKE. No credential files or secrets are needed — the pod's Kubernetes Service Account is used automatically.

1. Enable Workload Identity on your GKE cluster — [Enable Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#enable).
2. Create a Google Service Account (GSA) with "Storage Object Viewer" role.
3. Bind your Kubernetes Service Account (KSA) to the GSA:

   ```bash
   gcloud iam service-accounts add-iam-policy-binding GSA_EMAIL \
     --role roles/iam.workloadIdentityUser \
     --member "serviceAccount:PROJECT_ID.svc.id.goog[NAMESPACE/KSA_NAME]"
   kubectl annotate serviceaccount KSA_NAME \
     --namespace NAMESPACE \
     iam.gke.io/gcp-service-account=GSA_EMAIL
   ```

4. Set `auth_type: workload_identity` in your recipe. No `credential` or WIF config fields are needed.

#### Workload Identity Federation

Use this when DataHub runs outside GCP and you want keyless authentication without distributing service account key files.

1. Set up a Workload Identity Pool and Provider in Google Cloud — [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation).
2. Grant the external identity permission to impersonate a GCP service account with "Storage Object Viewer" role.
3. Download or generate the WIF credential configuration JSON from the Google Cloud Console.
4. Supply the configuration to the connector via one of three options: a file path (`gcp_wif_configuration`), inline dict (`gcp_wif_configuration_json`), or JSON string (`gcp_wif_configuration_json_string`). The JSON string option is useful for injecting from a secrets manager.


### Install the Plugin
```shell
pip install 'acryl-datahub[gcs]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# HMAC Authentication (default)
source:
  type: gcs
  config:
    path_specs:
      - include: gs://gcs-ingestion-bucket/parquet_example/{table}/year={partition[0]}/*.parquet
    credential:
      hmac_access_id: <hmac access id>
      hmac_access_secret: <hmac access secret>

---

# GKE Workload Identity (Application Default Credentials)
# Recommended for DataHub running inside GKE with Workload Identity enabled.
# No credential config needed — credentials are sourced automatically from the pod's service account.
source:
  type: gcs
  config:
    auth_type: workload_identity
    path_specs:
      - include: gs://gcs-ingestion-bucket/parquet_example/{table}/year={partition[0]}/*.parquet

---

# Workload Identity Federation with configuration file
source:
  type: gcs
  config:
    auth_type: workload_identity_federation
    gcp_wif_configuration: "/path/to/gcp_wif_configuration.json"
    path_specs:
      - include: gs://gcs-ingestion-bucket/parquet_example/{table}/year={partition[0]}/*.parquet

---

# Workload Identity Federation with inline configuration (dict)
source:
  type: gcs
  config:
    auth_type: workload_identity_federation
    gcp_wif_configuration_json:
      type: external_account
      audience: "//iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID"
      subject_token_type: "urn:ietf:params:oauth:token-type:jwt"
      token_url: "https://sts.googleapis.com/v1/token"
      credential_source:
        file: "/var/run/secrets/tokens/gcp-ksa/token"
      service_account_impersonation_url: "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT_EMAIL:generateAccessToken"
    path_specs:
      - include: gs://gcs-ingestion-bucket/parquet_example/{table}/year={partition[0]}/*.parquet

---

# Workload Identity Federation with JSON string (copy-paste from file)
source:
  type: gcs
  config:
    auth_type: workload_identity_federation
    gcp_wif_configuration_json_string: |
      {
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {
          "file": "/var/run/secrets/tokens/gcp-ksa/token"
        },
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT_EMAIL:generateAccessToken"
      }
    path_specs:
      - include: gs://gcs-ingestion-bucket/parquet_example/{table}/year={partition[0]}/*.parquet

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">path_specs</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of PathSpec. See [below](#path-spec) the details about PathSpec  |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">PathSpec</span></div> <div className="type-name-line"><span className="type-name">PathSpec</span></div> |   |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">include</span>&nbsp;<abbr title="Required if PathSpec is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.  |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">allow_double_stars</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Allow double stars in the include path. This can affect performance significantly if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">autodetect_partitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Autodetect partition(s) from the path. If set to true, it will autodetect partition key/value if the folder format is {partition_key}={partition_value} for example `year=2024` <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">default_extension</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">emit_folders_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If set, this path_spec emits each matching storage folder as a DataHub Container and creates no dataset entities. Folder depth is set by the number of wildcard levels in `include` (e.g. `.../*/` one level deep, `.../*/*/` two levels). `exclude` and `include_hidden_folders` still apply to the folder walk; file/dataset fields (`file_types`, `default_extension`, `table_name`, `tables_filter_pattern`) are rejected since no datasets are produced. `include` must end at a folder and must not contain `{table}` or `**`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">enable_compression</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable or disable processing compressed files. Currently .gz and .bz files are supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">include_hidden_folders</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include hidden folders in the traversal (folders starting with . or _ <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">sample_files</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">table_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Display name of the dataset.Combination of named variables from include path and strings <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">traversal_method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "ALL", "MIN_MAX", "MAX"  |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">exclude</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | list of paths in glob pattern which will be excluded while scanning for the datasets <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.exclude.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">file_types</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;csv&#x27;, &#x27;tsv&#x27;, &#x27;json&#x27;, &#x27;parquet&#x27;, &#x27;avro&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.file_types.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.</span><span className="path-main">tables_filter_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">path_specs.PathSpec.tables_filter_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">auth_type</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "hmac", "workload_identity_federation", "workload_identity"  |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">gcp_wif_configuration</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the GCP Workload Identity Federation configuration JSON file. Mutually exclusive with gcp_wif_configuration_json and gcp_wif_configuration_json_string. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">gcp_wif_configuration_json</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | GCP Workload Identity Federation configuration as a dict or a JSON string. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json_string. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">gcp_wif_configuration_json_string</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | GCP Workload Identity Federation configuration as a JSON string (contents of the configuration file). Useful for injecting configuration from secrets managers. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json. Note: WIF configuration typically contains public endpoint URLs rather than private keys, so SecretStr masking is not applied. If your WIF config contains sensitive material, ensure it is not logged at DEBUG level. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">max_rows</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of rows to use when inferring schemas for TSV and CSV files. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">number_of_files_to_sample</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of HMACKey, null</span></div> | Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys). Required when auth_type is 'hmac'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">hmac_access_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Access ID  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">hmac_access_secret</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Secret  |
| <div className="path-line"><span className="path-main">profile_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profile_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">profile_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">profile_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">profile_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">DataLakeProfilerConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


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
    "DataLakeProfilerConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_median_value": {
          "default": true,
          "description": "Whether to profile for the median value of numeric columns.",
          "title": "Include Field Median Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": true,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": true,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": true,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        }
      },
      "title": "DataLakeProfilerConfig",
      "type": "object"
    },
    "FolderTraversalMethod": {
      "enum": [
        "ALL",
        "MIN_MAX",
        "MAX"
      ],
      "title": "FolderTraversalMethod",
      "type": "string"
    },
    "GCSAuthType": {
      "enum": [
        "hmac",
        "workload_identity_federation",
        "workload_identity"
      ],
      "title": "GCSAuthType",
      "type": "string"
    },
    "HMACKey": {
      "additionalProperties": false,
      "properties": {
        "hmac_access_id": {
          "description": "Access ID",
          "title": "Hmac Access Id",
          "type": "string"
        },
        "hmac_access_secret": {
          "description": "Secret",
          "format": "password",
          "title": "Hmac Access Secret",
          "type": "string",
          "writeOnly": true
        }
      },
      "required": [
        "hmac_access_id",
        "hmac_access_secret"
      ],
      "title": "HMACKey",
      "type": "object"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
      "type": "object"
    },
    "PathSpec": {
      "additionalProperties": false,
      "properties": {
        "include": {
          "description": "Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.",
          "title": "Include",
          "type": "string"
        },
        "exclude": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": [],
          "description": "list of paths in glob pattern which will be excluded while scanning for the datasets",
          "title": "Exclude"
        },
        "file_types": {
          "default": [
            "csv",
            "tsv",
            "json",
            "parquet",
            "avro"
          ],
          "description": "Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted.",
          "items": {
            "type": "string"
          },
          "title": "File Types",
          "type": "array"
        },
        "default_extension": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped.",
          "title": "Default Extension"
        },
        "table_name": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Display name of the dataset.Combination of named variables from include path and strings",
          "title": "Table Name"
        },
        "enable_compression": {
          "default": true,
          "description": "Enable or disable processing compressed files. Currently .gz and .bz files are supported.",
          "title": "Enable Compression",
          "type": "boolean"
        },
        "sample_files": {
          "default": true,
          "description": "Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled",
          "title": "Sample Files",
          "type": "boolean"
        },
        "allow_double_stars": {
          "default": false,
          "description": "Allow double stars in the include path. This can affect performance significantly if enabled",
          "title": "Allow Double Stars",
          "type": "boolean"
        },
        "autodetect_partitions": {
          "default": true,
          "description": "Autodetect partition(s) from the path. If set to true, it will autodetect partition key/value if the folder format is {partition_key}={partition_value} for example `year=2024`",
          "title": "Autodetect Partitions",
          "type": "boolean"
        },
        "traversal_method": {
          "$ref": "#/$defs/FolderTraversalMethod",
          "default": "MAX",
          "description": "Method to traverse the folder. ALL: Traverse all the folders, MIN_MAX: Traverse the folders by finding min and max value, MAX: Traverse the folder with max value"
        },
        "include_hidden_folders": {
          "default": false,
          "description": "Include hidden folders in the traversal (folders starting with . or _",
          "title": "Include Hidden Folders",
          "type": "boolean"
        },
        "tables_filter_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "The tables_filter_pattern configuration field uses regular expressions to filter the tables part of the Pathspec for ingestion, allowing fine-grained control over which tables are included or excluded based on specified patterns. The default setting allows all tables."
        },
        "emit_folders_only": {
          "default": false,
          "description": "If set, this path_spec emits each matching storage folder as a DataHub Container and creates no dataset entities. Folder depth is set by the number of wildcard levels in `include` (e.g. `.../*/` one level deep, `.../*/*/` two levels). `exclude` and `include_hidden_folders` still apply to the folder walk; file/dataset fields (`file_types`, `default_extension`, `table_name`, `tables_filter_pattern`) are rejected since no datasets are produced. `include` must end at a folder and must not contain `{table}` or `**`.",
          "title": "Emit Folders Only",
          "type": "boolean"
        }
      },
      "required": [
        "include"
      ],
      "title": "PathSpec",
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
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "path_specs": {
      "description": "List of PathSpec. See [below](#path-spec) the details about PathSpec",
      "items": {
        "$ref": "#/$defs/PathSpec"
      },
      "title": "Path Specs",
      "type": "array"
    },
    "gcp_wif_configuration": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the GCP Workload Identity Federation configuration JSON file. Mutually exclusive with gcp_wif_configuration_json and gcp_wif_configuration_json_string.",
      "title": "Gcp Wif Configuration"
    },
    "gcp_wif_configuration_json": {
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
      "description": "GCP Workload Identity Federation configuration as a dict or a JSON string. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json_string.",
      "title": "Gcp Wif Configuration Json"
    },
    "gcp_wif_configuration_json_string": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "GCP Workload Identity Federation configuration as a JSON string (contents of the configuration file). Useful for injecting configuration from secrets managers. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json. Note: WIF configuration typically contains public endpoint URLs rather than private keys, so SecretStr masking is not applied. If your WIF config contains sensitive material, ensure it is not logged at DEBUG level.",
      "title": "Gcp Wif Configuration Json String"
    },
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
      "default": null
    },
    "auth_type": {
      "$ref": "#/$defs/GCSAuthType",
      "default": "hmac",
      "description": "Authentication type to use. Defaults to 'hmac'. Set to 'workload_identity_federation' to authenticate via a WIF configuration file (for external workloads). Set to 'workload_identity' to use Application Default Credentials \u2014 the recommended option when running inside GKE with Workload Identity enabled."
    },
    "credential": {
      "anyOf": [
        {
          "$ref": "#/$defs/HMACKey"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys). Required when auth_type is 'hmac'."
    },
    "max_rows": {
      "default": 100,
      "description": "Maximum number of rows to use when inferring schemas for TSV and CSV files.",
      "title": "Max Rows",
      "type": "integer"
    },
    "number_of_files_to_sample": {
      "default": 100,
      "description": "Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec.",
      "title": "Number Of Files To Sample",
      "type": "integer"
    },
    "profile_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for tables to profile "
    },
    "profiling": {
      "$ref": "#/$defs/DataLakeProfilerConfig",
      "default": {
        "enabled": false,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        },
        "profile_table_level_only": false,
        "max_number_of_fields_to_profile": null,
        "include_field_null_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true,
        "include_field_mean_value": true,
        "include_field_median_value": true,
        "include_field_stddev_value": true,
        "include_field_quantiles": true,
        "include_field_distinct_value_frequencies": true,
        "include_field_histogram": true,
        "include_field_sample_values": true
      },
      "description": "Data profiling configuration"
    }
  },
  "required": [
    "path_specs"
  ],
  "title": "GCSSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Path Specs

**Example - Dataset per file**

Bucket structure:

```
test-gs-bucket
├── employees.csv
└── food_items.csv
```

Path specs config

```
path_specs:
    - include: gs://test-gs-bucket/*.csv

```

**Example - Datasets with partitions**

Bucket structure:

```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── returns
    └── year=2021
        └── month=2
            └── 1.parquet

```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Example - Datasets with partition and exclude**

Bucket structure:

```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── tmp_orders
    └── year=2021
        └── month=2
            └── 1.parquet


```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
      exclude:
        - **/tmp_orders/**
```

**Example - Datasets of mixed nature**

Bucket structure:

```
test-gs-bucket
├── customers
│   ├── part1.json
│   ├── part2.json
│   ├── part3.json
│   └── part4.json
├── employees.csv
├── food_items.csv
├── tmp_10101000.csv
└──  orders
    └── year=2022
        └── month=2
            ├── 1.parquet
            ├── 2.parquet
            └── 3.parquet

```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/*.csv
      exclude:
        - **/tmp_10101000.csv
    - include: gs://test-gs-bucket/{table}/*.json
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Valid path_specs.include**

```python
gs://my-bucket/foo/tests/bar.avro # single file table
gs://my-bucket/foo/tests/*.* # mulitple file level tables
gs://my-bucket/foo/tests/{table}/*.avro #table without partition
gs://my-bucket/foo/tests/{table}/*/*.avro #table where partitions are not specified
gs://my-bucket/foo/tests/{table}/*.* # table where no partitions as well as data type specified
gs://my-bucket/{dept}/tests/{table}/*.avro # specifying keywords to be used in display name
gs://my-bucket/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro # specify partition key and value format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro # specify partition value only format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # for all extensions
gs://my-bucket/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 2 levels down in bucket
gs://my-bucket/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 3 levels down in bucket
```

**Valid path_specs.exclude**

- \*\*/tests/\*\*
- gs://my-bucket/hr/\*\*
- \*_/tests/_.csv
- gs://my-bucket/foo/\*/my_table/\*\*

**Notes**

- `{table}` represents folder for which dataset will be created.
- include path must end with (`*.*` or `*.[ext]`) to represent leaf level.
- if `*.[ext]` is provided then only files with specified type will be scanned.
- `/*/ `represents single folder.
- `{partition[i]}` represents value of partition.
- `{partition_key[i]}` represents name of the partition.
- While extracting, "i" will be used to match partition_key to partition.
- all folder levels need to be specified in include. Only exclude path can have `**` like matching.
- exclude path cannot have named variables ( {} ).
- Folder names should not contain {, }, \*, / in their names.
- {folder} is reserved for internal working. please do not use in named variables.

If you would like to write a more complicated function for resolving file names, then a {transformer} would be a good fit.

:::caution

Specify as long fixed prefix ( with out /\*/ ) as possible in `path_specs.include`. This will reduce the scanning time and cost, specifically on Google Cloud Storage.

:::

:::caution

If you are ingesting datasets from Google Cloud Storage, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Supported file types

Supported file types are as follows:

- CSV
- TSV
- JSONL
- JSON
- Parquet
- Apache Avro

Schemas for Parquet and Avro files are extracted as provided.

Schemas for schemaless formats (CSV, TSV, JSONL, JSON) are inferred. For CSV, TSV and JSONL files, we consider the first 100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few objects of the file), which may impact performance.
We are working on using iterator-based JSON parsers to avoid reading in the entire JSON object.

#### Profiling

Profiling is supported for GCS and, when enabled, extracts:

- Row and column counts for each dataset
- For each column, if profiling is enabled:
  - null counts and proportions
  - distinct counts and proportions
  - minimum, maximum, mean, median, standard deviation, some quantile values
  - histograms or frequencies of unique values

Profiling is a pure-Python implementation (built on `pyarrow` and Apache DataSketches) and requires no Spark, Hadoop, or JVM. Distinct counts and quantiles/histograms are approximate (DataSketches). GCS files are read over the S3-interoperability endpoint using the same credentials configured for ingestion, so no extra setup is needed beyond enabling profiling.

Enabling profiling will slow down ingestion runs.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.gcs.gcs_source.GCSSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/gcs/gcs_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Google Cloud Storage, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
