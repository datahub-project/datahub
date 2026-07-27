### Overview

The `hana` module ingests metadata from SAP HANA into DataHub. It targets both SAP HANA Cloud and on-premise HANA tenants and is intended for production ingestion workflows.

The connector extends DataHub's `SQLAlchemySource`, so the standard SQL extraction path (tables, regular views, schema reflection, profiling, classification, stateful deletion detection, container hierarchy, test-connection) is the same as for other SQLAlchemy-backed sources. On top of that, three HANA-specific paths can be enabled:

- **Calculation Views** — set `include_calculation_views: true` to pull activated calculation views from `_SYS_REPO.ACTIVE_OBJECT` and parse column-level lineage from their XML definitions. Filter the set with `calculation_view_pattern` (matched against `<package_id>.<view_name>`); the inherited `view_pattern` filters regular SQL views and does not apply here.
- **Stored Procedures** — enabled by default via `include_stored_procedures: true`. Each procedure becomes a `DataJob` grouped under a per-schema `DataFlow`; lineage is parsed from the procedure body.
- **Query Usage** — set `include_query_usage: true` to mine `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE` for observed queries and feed them through the SQL parsing aggregator. Combine with `include_usage_stats: true` for `DatasetUsageStatistics` rollups and `include_operational_stats: true` for read operations.

#### Deployment compatibility

Most features work on every supported HANA tenant, but **calculation-view extraction is on-premise / self-managed only**:

| Capability                                          | SAP HANA on-premise / self-managed (HANA 1.0 SPS12+, HANA 2.0, HANA Express) | SAP HANA Cloud / HDI-only deployments                                                                                                                            |
| --------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tables, regular views, schema reflection            | ✅                                                                           | ✅                                                                                                                                                               |
| Stored procedures (`include_stored_procedures`)     | ✅                                                                           | ✅                                                                                                                                                               |
| Query usage (`include_query_usage`)                 | ✅                                                                           | ✅                                                                                                                                                               |
| Profiling                                           | ✅                                                                           | ✅                                                                                                                                                               |
| **Calculation views (`include_calculation_views`)** | ✅                                                                           | ❌ — HANA Cloud does not ship the XS-classic repository (`_SYS_REPO`); calc views are deployed via HDI containers and are not exposed as parseable XML payloads. |

If you run `include_calculation_views: true` against HANA Cloud, the calc-view extractor logs a warning and falls back to a no-op rather than failing the run, so the rest of metadata ingestion completes normally.

### Prerequisites

#### Database driver

The connector uses SAP's official [`hdbcli`](https://pypi.org/project/hdbcli/) driver via [`sqlalchemy-hana`](https://github.com/SAP/sqlalchemy-hana). Both are installed automatically when you install the `hana` extra.

> **Architecture note:** `hdbcli` ships precompiled wheels for `x86_64` only. On `aarch64` / `arm64` (Apple Silicon, AWS Graviton) you must run the ingestion process under an x86_64 Python interpreter (e.g. via Rosetta or an x86_64 container).

#### Network connectivity

The default port for the SAP `hdbcli` driver is `30015` (single-container instances) or the tenant port surfaced in HANA Cockpit (multitenant database container deployments). Ensure the ingestion process can reach the tenant on that port.

#### Permissions

Run the following as a HANA administrator (a user with `ROLE ADMIN` and `USER ADMIN`) to create a DataHub-specific role and user. Grants are layered by capability — comment out the sections for features you don't intend to enable.

```sql
-- 1. Role + user
CREATE ROLE DATAHUB_ROLE;

CREATE USER DATAHUB_USER PASSWORD "<your-strong-password>" NO FORCE_FIRST_PASSWORD_CHANGE;
GRANT DATAHUB_ROLE TO DATAHUB_USER;

-- 2. Baseline metadata: tables, regular views, schema reflection.
--    Repeat for every schema you want to ingest.
GRANT SELECT ON SCHEMA "<YOUR_SCHEMA>" TO DATAHUB_ROLE;

-- 3. Stored procedures (required when `include_stored_procedures: true`,
--    which is the default).
GRANT SELECT ON SYS.PROCEDURES TO DATAHUB_ROLE;
GRANT SELECT ON SYS.PROCEDURE_PARAMETERS TO DATAHUB_ROLE;

-- 4. Calculation views — ON-PREMISE / SELF-MANAGED ONLY.
--    Requires the SAP HANA XS-classic repository (`_SYS_REPO`). Skip this
--    block entirely on SAP HANA Cloud / HDI-only deployments.
GRANT SELECT ON _SYS_REPO.ACTIVE_OBJECT TO DATAHUB_ROLE;
GRANT SELECT ON SYS.VIEW_COLUMNS TO DATAHUB_ROLE;
GRANT SELECT ON SCHEMA _SYS_BIC TO DATAHUB_ROLE;

-- 5. Query usage from `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`
--    (required when `include_query_usage: true`).
--    `MONITORING` is the lowest-privilege role that covers it; alternatively
--    use the broader `CATALOG READ` system privilege.
GRANT MONITORING TO DATAHUB_ROLE;
-- or, equivalently:
-- GRANT CATALOG READ TO DATAHUB_ROLE;

-- 6. Profiling (only required when `profiling.enabled: true`).
--    Profiling reads sample data from the tables it inspects, so it inherits
--    the SELECT grants from step 2 — no extra privileges needed beyond that.
```

#### Capability-to-grant matrix

| Capability                                                        | Required grants                                                              | Notes                                                                                                                            |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Tables, regular views, schema reflection                          | `SELECT` on each ingested schema                                             | Grant per schema you want to ingest. Without this the schema is invisible to `SQLAlchemy` reflection.                            |
| Stored procedures (`include_stored_procedures: true`, default on) | `SELECT` on `SYS.PROCEDURES`, `SYS.PROCEDURE_PARAMETERS`                     | Stored-procedure bodies and signatures are read from `SYS.PROCEDURES`; argument signatures use `SYS.PROCEDURE_PARAMETERS`.       |
| Calculation views (`include_calculation_views: true`)             | `SELECT` on `_SYS_REPO.ACTIVE_OBJECT`, `SYS.VIEW_COLUMNS`, `SCHEMA _SYS_BIC` | **On-premise / self-managed HANA only.** `_SYS_BIC` is the runtime schema activated calc views materialise into.                 |
| Query usage (`include_query_usage: true`)                         | `MONITORING` role _or_ `CATALOG READ` system privilege                       | Both grant read access to `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`. The statistics service must also be running (it is by default). |
| Profiling (`profiling.enabled: true`)                             | Inherits per-schema `SELECT` from row 1                                      | Profiling queries the same tables visible to metadata extraction; no additional grants required.                                 |

The `MONITORING` role is included with HANA's standard role catalog (see [SAP note 2147247](https://userapps.support.sap.com/sap/support/knowledge/en/2147247) for diagnostics if `_SYS_STATISTICS` looks empty after granting it).
