# Nifi

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[nifi]'`.

## Capabilities

This plugin extracts the following:

- Nifi flow as `DataFlow` entity
- Ingress, egress processors, remote input and output ports as `DataJob` entity
- Input and output ports receiving remote connections as `Dataset` entity
- Lineage information between external datasets and ingress/egress processors by analyzing provenance events

Current limitations:

- Limited ingress/egress processors are supported
  - S3: `ListS3`, `FetchS3Object`, `PutS3Object`
  - SFTP: `ListSFTP`, `FetchSFTP`, `GetSFTP`, `PutSFTP`

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "nifi"
  config:
    # Coordinates
    site_url: "https://localhost:8443/nifi/"
    
    # Credentials
    auth: SINGLE_USER    
    username: admin
    password: password

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                      | Required | Default                    | Description                                             |
| -------------------------- | -------- | -------------------------- | ------------------------------------------------------- |
| `site_url`              |    ✅    |                                 | URI to connect                                                                           |
| `site_name`             |          | `"default"`                      | Site name to identify this site with, useful when using input and output ports receiving remote connections                                                                       |
| `auth`                  |          |           `"NO_AUTH"`            | Nifi authentication. must be one of : NO_AUTH, SINGLE_USER, CLIENT_CERT                                |
| `username`                  |          |                              | Nifi username, must be set for `auth` = `"SINGLE_USER"`                                                  |
| `password`                  |          |                              | Nifi password, must be set for `auth` = `"SINGLE_USER"`                                                  |
| `client_cert_file`          |          |                              | Path to PEM file containing the public certificates for the user/client identity, must be set for `auth` = `"CLIENT_CERT"`                                                                   |
| `client_key_file`           |          |                              | Path to PEM file containing the client’s secret key                                                |
| `client_key_password`       |          |                              | The password to decrypt the client_key_file                                                               |
| `ca_file`                   |          |                             | Path to PEM file containing certs for the root CA(s) for the NiFi                                  |
| `provenance_days`           |          |                             | time window to analyze provenance events for external datasets                                           |
| `site_url_to_site_name`     |          |                             | Lookup to find site_name for site_url, required if using remote process groups in nifi  flow               |
|`process_group_pattern.allow`|          |                             | List of regex patterns for process groups to include in ingestion.                                       |
| `process_group_pattern.deny`|          |                             | List of regex patterns for process groups to exclude from ingestion.                                     |
| `process_group_pattern.ignoreCase`  |  |          `True`             | Whether to ignore case sensitivity during pattern matching.                                              |
| `env`                       |          |           `"PROD"`          | Environment to use in namespace when constructing URNs.                                                 |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
