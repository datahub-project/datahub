### Concept Mapping

The following table shows how Dremio entities are mapped to DataHub concepts:

| Source Concept             | DataHub Concept | Notes                                                      |
| -------------------------- | --------------- | ---------------------------------------------------------- |
| **Physical Dataset/Table** | `Dataset`       | Subtype: `Table`                                           |
| **Virtual Dataset/Views**  | `Dataset`       | Subtype: `View`                                            |
| **Spaces**                 | `Container`     | Mapped to DataHub's `Container` aspect. Subtype: `Space`   |
| **Folders**                | `Container`     | Mapped as a `Container` in DataHub. Subtype: `Folder`      |
| **Sources**                | `Container`     | Represented as a `Container` in DataHub. Subtype: `Source` |

### Required Dremio Privileges

The DataHub connector requires the following Dremio privileges:

**Minimum Required:**

- **`READ METADATA ON SYSTEM`**: Access to view containers (sources/spaces) and their metadata via API
- **`SELECT ON SYSTEM`**: Access to system tables (`SYS.*`, `INFORMATION_SCHEMA.*`) for dataset metadata

**Optional (feature-dependent):**

- **`SELECT ON SOURCE/SPACE`**: Access to read actual data (only needed if data profiling is enabled)
- **`VIEW JOB HISTORY`**: Access to query history tables (only needed if `include_query_lineage: true`)

**Important**: The connector uses both REST API calls and SQL queries, each requiring appropriate privileges on their respective objects.
