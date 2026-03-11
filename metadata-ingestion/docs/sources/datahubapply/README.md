## Overview

DataHub Apply is a DataHub utility or metadata-focused integration. Learn more in the [official DataHub Apply documentation](https://datahub.com/docs/).

The DataHub integration for DataHub Apply covers metadata entities and operational objects relevant to this connector. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Source Concept                             | DataHub Concept                                      | Notes                                                    |
| ------------------------------------------ | ---------------------------------------------------- | -------------------------------------------------------- |
| Apply operation input                      | Metadata Change Proposal (MCP) updates               | Input drives metadata updates rather than discovery.     |
| Asset target list                          | Dataset / Container (and other supported entities)   | Targets are selected explicitly in recipe configuration. |
| Ownership / domain / tag / term assignment | Ownership, Domain, GlobalTags, GlossaryTerms aspects | Applied directly to existing DataHub entities.           |
