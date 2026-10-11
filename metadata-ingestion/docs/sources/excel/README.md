## Overview

Excel is a storage and lakehouse platform. Learn more in the [official Excel documentation](https://www.microsoft.com/microsoft-365/excel).

The DataHub integration for Excel covers file/lakehouse metadata entities such as datasets, paths, and containers. It also captures data profiling and stateful deletion detection.

## Concept Mapping

| Excel Entity                 | DataHub Entity | Description                                                                                                                          |
| ---------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| **Excel Worksheet**          | **Dataset**    | Each worksheet becomes a dataset with URN pattern: `urn:li:dataset:(urn:li:dataPlatform:excel,{path}/[{filename}]{sheet_name},PROD)` |
| **File/Directory Structure** | **Container**  | Directory hierarchy creates containers with obfuscated URNs for organizing datasets                                                  |

:::info Excel workbook

The Excel workbook file itself does not become a separate DataHub entity - only the individual worksheets within it are ingested as datasets.
:::
