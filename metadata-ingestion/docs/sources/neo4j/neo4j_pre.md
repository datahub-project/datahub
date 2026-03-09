### Overview

The `neo4j` module ingests metadata from Neo4J into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

Neo4j metadata will be ingested into DataHub using
`CALL apoc.meta.schema() YIELD value UNWIND keys(value) AS key RETURN key, value[key] AS value;`  
The data that is returned will be parsed
and will be displayed as Nodes and Relationships in DataHub. Each object will be tagged with describing what kind of DataHub
object it is. The defaults are 'Node' and 'Relationship'. These tag values can be overwritten in the recipe.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

In order to ingest metadata from Neo4j, you will need:

- Neo4j instance with APOC installed
