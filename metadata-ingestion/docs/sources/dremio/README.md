## Overview

Dremio is a DataHub utility or metadata-focused integration.

The DataHub integration for Dremio covers metadata entities and operational objects relevant to this connector. It also captures table- and column-level lineage, usage statistics, data profiling, ownership, and stateful deletion detection.

## Concept Mapping

| Source Concept             | DataHub Concept | Notes                                                      |
| -------------------------- | --------------- | ---------------------------------------------------------- |
| **Physical Dataset/Table** | `Dataset`       | Subtype: `Table`                                           |
| **Virtual Dataset/Views**  | `Dataset`       | Subtype: `View`                                            |
| **Spaces**                 | `Container`     | Mapped to DataHub’s `Container` aspect. Subtype: `Space`   |
| **Folders**                | `Container`     | Mapped as a `Container` in DataHub. Subtype: `Folder`      |
| **Sources**                | `Container`     | Represented as a `Container` in DataHub. Subtype: `Source` |
