## Overview

DataHub Apply is a utility-style platform connector used to apply metadata changes directly within DataHub (for example ownership, domains, tags, and glossary terms) as part of managed ingestion workflows.

The `datahub-apply` module is intended for controlled metadata curation and bulk metadata application use cases where the source of truth is already known.

## Concept Mapping

| Source Concept                             | DataHub Concept                                      | Notes                                                    |
| ------------------------------------------ | ---------------------------------------------------- | -------------------------------------------------------- |
| Apply operation input                      | Metadata Change Proposal (MCP) updates               | Input drives metadata updates rather than discovery.     |
| Asset target list                          | Dataset / Container (and other supported entities)   | Targets are selected explicitly in recipe configuration. |
| Ownership / domain / tag / term assignment | Ownership, Domain, GlobalTags, GlossaryTerms aspects | Applied directly to existing DataHub entities.           |
