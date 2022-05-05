### Prerequisites

In order to ingest metadata from Apache Pulsar, you will need:

* Access to a Pulsar Instance, if authentication is enabled a valid access token.
* Pulsar version >= 2.7.0
* ...

> **_NOTE:_**  A _superUser_ role is required for listing all existing tenants within a Pulsar instance.
>

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[pulsar]'`

### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion. See [below](#config-details) for full configuration options.

_For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes)._

#### Quickstart recipe
Getting started receipt
```yml
source:
  type: pulsar
  config:
    # Required fields
    web_service_url: "http://localhost:8080"

sink:
  # sink configs
```

> **_NOTE:_**  Always use TLS encryption in a production environment and use variable substitution for sensitive information (e.g. ${CLIENT_ID} and ${CLIENT_SECRET}).
>
