## Overview

DataHub Debug is a DataHub utility or metadata-focused integration. Learn more in the [official DataHub Debug documentation](https://datahub.com/docs/).

The DataHub integration for DataHub Debug covers metadata entities and operational objects relevant to this connector. It is a diagnostic utility for inspecting DataHub state and does not import additional source metadata.

## Concept Mapping

| Source Concept    | DataHub Concept                  | Notes                                                            |
| ----------------- | -------------------------------- | ---------------------------------------------------------------- |
| Debug query/input | DataHub entity/aspect inspection | Reads metadata for diagnostics.                                  |
| Diagnostic output | Operational debugging signal     | Used for troubleshooting and validation, not catalog enrichment. |
