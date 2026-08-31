### Overview

The `sagemaker` module ingests metadata from SageMaker into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Feature groups
- Models, jobs, and lineage between the two (e.g. when jobs output a model or a model is used by a job)

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
