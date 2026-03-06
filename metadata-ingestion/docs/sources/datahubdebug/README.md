## Overview

DataHub Debug is a utility-style connector used to inspect and diagnose DataHub metadata state and ingestion behavior.

The `datahub-debug` module is primarily intended for operational debugging workflows rather than broad metadata discovery.

## Concept Mapping

| Source Concept    | DataHub Concept                  | Notes                                                            |
| ----------------- | -------------------------------- | ---------------------------------------------------------------- |
| Debug query/input | DataHub entity/aspect inspection | Reads metadata for diagnostics.                                  |
| Diagnostic output | Operational debugging signal     | Used for troubleshooting and validation, not catalog enrichment. |
