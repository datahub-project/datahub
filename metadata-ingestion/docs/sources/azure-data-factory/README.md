## Overview

Azure Data Factory is a streaming or integration platform. Learn more in the [official Azure Data Factory documentation](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app).

The DataHub integration for Azure Data Factory covers streaming/integration entities such as topics, connectors, pipelines, or jobs. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| ADF Concept  | DataHub Entity      |
| ------------ | ------------------- |
| Data Factory | Container           |
| Pipeline     | DataFlow            |
| Activity     | DataJob             |
| Dataset      | Dataset             |
| Pipeline Run | DataProcessInstance |
