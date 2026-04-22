### Overview

The `kafka` module ingests metadata from Kafka into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Extract Topics & Schemas from Apache Kafka or Confluent Cloud.

This plugin extracts the following:

- Topics from the Kafka broker
- Schemas associated with each topic from the schema registry (Avro, Protobuf and JSON schemas are supported)

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
