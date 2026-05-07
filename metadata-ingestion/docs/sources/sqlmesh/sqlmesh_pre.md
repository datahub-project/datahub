### Overview

The `sqlmesh` source plugin reads SQLMesh project metadata directly from the SQLMesh Python
API—no running SQLMesh server is required. It emits Dataset entities on the
`urn:li:dataPlatform:sqlmesh` platform and links each one to its corresponding warehouse
view (Snowflake, BigQuery, DuckDB, etc.) as a sibling, so DataHub merges both into a
single unified view in the UI.

#### Sibling stitching

For sibling URNs to stitch correctly, the `sqlmesh` connector and your warehouse connector
must agree on the dataset name. Key config options:

- **`target_platform_instance`** — must match the `platform_instance` in your warehouse recipe exactly.
- **`default_catalog`** — required when SQLMesh model names are two-part (`schema.model`) but
  your warehouse connector emits three-part URNs (`catalog.schema.table`).
- **`convert_urns_to_lowercase`** — auto-enabled for Snowflake; set manually for other
  case-folding warehouses.

#### Column-level lineage

SQLMesh parses all SQL through [SQLGlot](https://github.com/tobymao/sqlglot), so
column-level lineage is available natively for all SQL models without any additional
configuration. Python models (ibis, pandas) do not have column-level lineage available.

#### Data quality audits

SQLMesh audit definitions (`not_null`, `unique_values`, `unique_combination_of_columns`,
`number_of_rows`, `forall`, `accepted_range`, `accepted_values`) are emitted as typed
DataHub [Assertion](https://docs.datahub.com/docs/managed-datahub/observe/assertions)
entities and appear on the **Validation** tab of each dataset.

To surface pass/fail run results on the Validation tab, produce a JSON results file
and point `audit_results_path` at it. The expected format is described in the config
reference below.

### Prerequisites

- Python 3.9 or later
- The `sqlmesh` Python package installed in the ingestion environment:
  `pip install 'acryl-datahub[sqlmesh]'`
- Read access to the SQLMesh project directory (config files and model SQL)
- If using a remote gateway (Snowflake, BigQuery, etc.), valid gateway credentials
  in the SQLMesh project config—the connector loads the SQLMesh context which opens
  a connection to resolve model metadata
