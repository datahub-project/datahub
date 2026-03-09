## Overview

OpenAPI is a documentation or collaboration platform. Learn more in the [official OpenAPI documentation](https://www.openapis.org/).

The DataHub integration for OpenAPI covers document/workspace entities and hierarchy context for knowledge assets. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Source Concept | DataHub Concept                                                                           | Notes                  |
| -------------- | ----------------------------------------------------------------------------------------- | ---------------------- |
| `"OpenAPI"`    | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                        |
| API Endpoint   | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            | Subtype `API_ENDPOINT` |
