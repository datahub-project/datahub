# Source Name 

<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Supported Capabilities

<!-- This should be an auto-generated table of supported DataHub features/functionality -->
<!-- Each capability should link out to a feature guide -->

| Capability | Status | Notes |
| --- | :-: | --- |
| Data Container | ✅ | Enabled by default | 
| Descriptions | ✅ | Enabled by default |
| Platform Instance | ✅ | Enabled by default |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from Amplitude, you will need:

* an account in Amplitude
* at least one project within that account that contains data
* api keys and secret keys for each project to ingest

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[amplitude]'`

### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion. 

#### `'acryl-datahub[amplitude]'`

```yml
source:
  type: "amplitude"
  config:
    connect_uri: "https://amplitude.com/api/2"
    projects:
      - name: project name
        description: project description
        api_key: project api key
        secret_key: project secret key
sink:
  # sink configs
```

Further projects can be added in the following way:

```yaml
source:
  type: "amplitude"
  config:
    connect_uri: "https://amplitude.com/api/2"
    projects:
      - name: project name
        description: project description
        api_key: project api key
        secret_key: project secret key
      - name: project name
        description: project description
        api_key: project api key
        secret_key: project secret key
sink:
  # sink configs
```
