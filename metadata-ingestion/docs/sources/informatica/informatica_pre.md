### Overview

The `informatica` module ingests metadata from Informatica Cloud (IDMC) into DataHub. It extracts projects, folders, taskflows, mappings, and mapping tasks, and resolves table-level lineage from mapping source/target connections.

:::tip Quick Start

1. **Create a service account** — Use a dedicated IDMC user with minimum permissions (see [Required Permissions](#required-permissions))
2. **Identify your pod URL** — Determine the IDMC regional login URL (US, US2, EMEA, or APAC)
3. **Configure recipe** — Use `informatica_recipe.yml` as a template
4. **Run ingestion** — Execute `datahub ingest -c informatica_recipe.yml`

:::

#### Key Features

- Projects and folders as Containers
- Taskflows as DataFlows; Mappings and Mapping Tasks as DataJobs
- Table-level lineage (source → mapping → target) resolved via the v3 Export API and connection metadata
- Three-layer filtering: tag-based (recommended for large orgs), project/folder pattern, and mapping/taskflow name pattern
- Cross-source lineage to datasets ingested by other connectors (Snowflake, Oracle, BigQuery, etc.) via connection type mapping
- Manual connection type overrides for unusual or custom connectors
- Stateful ingestion for stale entity removal
- Ownership extraction from `createdBy`/`updatedBy`

#### Concept Mapping

| IDMC concept  | DataHub entity                        | Subtype        |
| ------------- | ------------------------------------- | -------------- |
| Project       | Container                             | `Project`      |
| Folder        | Container                             | `Folder`       |
| Taskflow      | DataFlow                              | `Taskflow`     |
| Mapping (v3)  | DataJob                               | `Mapping`      |
| Mapping Task  | DataJob                               | `Mapping Task` |
| Source/target | Dataset (upstream/downstream lineage) | —              |

Mappings are grouped under a synthetic per-project DataFlow so lineage navigation in the UI remains scoped to the project that owns them. Mapping Tasks are each attached to their own DataFlow because they schedule a single mapping with its own parameters.

### Prerequisites

#### Required Permissions

| Capability                        | IDMC privilege                        | Notes                                                            |
| --------------------------------- | ------------------------------------- | ---------------------------------------------------------------- |
| Authenticate                      | Any active IDMC user                  | Uses the v2 login endpoint                                       |
| List projects, folders, taskflows | `Asset - read` (or the Observer role) | Needed for all container/flow emission                           |
| List mappings / mapping tasks     | `Asset - read`                        | Mapping Tasks are optional and skipped with a warning if 403     |
| Extract table-level lineage       | `Asset - export`                      | Submits v3 export jobs; skip by setting `extract_lineage: false` |
| List connections                  | `Connection - read`                   | Needed for lineage to resolve to dataset URNs                    |

#### Regional login URLs

Set `login_url` to your IDMC pod's regional URL (not the API runtime URL — the connector discovers that from the login response):

| Region | `login_url`                           |
| ------ | ------------------------------------- |
| US     | `https://dm-us.informaticacloud.com`  |
| US2    | `https://dm2-us.informaticacloud.com` |
| EMEA   | `https://dm-em.informaticacloud.com`  |
| APAC   | `https://dm-ap.informaticacloud.com`  |

#### References

- [Informatica IDMC REST API](https://docs.informatica.com/integration-cloud/data-integration/current-version/rest-api-reference.html)
- [IDMC v3 Objects API](https://docs.informatica.com/integration-cloud/data-integration/current-version/rest-api-reference/platform-rest-api-version-3-resources/objects.html)
- [IDMC Export API](https://docs.informatica.com/integration-cloud/data-integration/current-version/rest-api-reference/platform-rest-api-version-3-resources/export.html)
