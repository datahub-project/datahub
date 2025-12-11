<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

> **_NOTE:_** Always use TLS encryption in a production environment and use variable substitution for sensitive information (e.g. ${CLIENT_ID} and ${CLIENT_SECRET}).

### Prerequisites

In order to ingest metadata from Apache Pulsar, you will need:

- Access to a Pulsar Instance, if authentication is enabled a valid access token.
- Pulsar version >= 2.7.0

> **_NOTE:_** A _superUser_ role is required for listing all existing tenants within a Pulsar instance.
