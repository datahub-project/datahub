### Overview

The `kafka-connect` module ingests metadata from Kafka Connect into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Source and Sink Connectors in Kafka Connect as Data Pipelines
- For Source connectors - Data Jobs to represent lineage information between source dataset to Kafka topic per `{connector_name}:{source_dataset}` combination
- For Sink connectors - Data Jobs to represent lineage information between Kafka topic to destination dataset per `{connector_name}:{topic}` combination

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Java Runtime Dependency

This source requires Java to be installed and available on the system for transform pipeline support (RegexRouter, etc.). The Java runtime is accessed via JPype to enable Java regex pattern matching that's compatible with Kafka Connect transforms.

- **Python installations**: Install Java separately (e.g., `apt-get install openjdk-11-jre-headless` on Debian/Ubuntu)
- **Docker deployments**: Ensure your DataHub ingestion Docker image includes a Java runtime. The official DataHub images include Java by default.
- **Impact**: Without Java, transform pipeline features will be disabled and lineage accuracy may be reduced for connectors using transforms

**Note for Docker users**: If you're building custom Docker images for DataHub ingestion, ensure a Java Runtime Environment (JRE) is included in your image to support full transform pipeline functionality.
