### Overview

The `datahub-lineage-file` module ingests metadata from File Based Lineage into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin pulls lineage metadata from a yaml-formatted file. An example of one such file is located in the examples directory [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/bootstrap_data/file_lineage.yml).

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
