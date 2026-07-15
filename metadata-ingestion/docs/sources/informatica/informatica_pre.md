### Overview

The `informatica` module ingests metadata from Informatica Cloud (IDMC) into DataHub. It extracts projects, folders, Mapping Tasks, and Taskflows, and resolves table-level lineage from the Mapping each Task references. Standalone Mappings (ones without a Mapping Task) and Mapplets are not emitted.

:::tip Quick Start

1. **Create a service account** — Use a dedicated IDMC user with minimum permissions (see [Required Permissions](#required-permissions))
2. **Identify your pod URL** — Determine the IDMC regional login URL (US, US2, EMEA, or APAC)
3. **Configure recipe** — Use `informatica_recipe.yml` as a template
4. **Run ingestion** — Execute `datahub ingest -c informatica_recipe.yml`

:::

#### Key Features

- Projects and folders as Containers
- Mapping Tasks as DataFlows with a `transform` DataJob each; Taskflows as DataFlows with one `orchestrate` DataJob that chains the MTs in step order
- Table-level lineage (source → mapping → target) resolved via the v3 Export API and connection metadata; Mapping Tasks chain to each other in Taskflow step order and the Taskflow `orchestrate` DataJob anchors the end of the chain
- Three-layer filtering: tag-based (recommended for large orgs), project/folder pattern, and mapping/taskflow name pattern
- Cross-source lineage to datasets ingested by other connectors (Snowflake, Oracle, BigQuery, etc.) via connection type mapping
- Manual connection type overrides for unusual or custom connectors
- Stateful ingestion for stale entity removal
- Ownership extraction from `createdBy`/`updatedBy`

#### Concept Mapping

| IDMC concept  | DataHub entity                             | Subtype                               |
| ------------- | ------------------------------------------ | ------------------------------------- |
| Project       | Container                                  | `Project`                             |
| Folder        | Container                                  | `Folder`                              |
| Taskflow      | DataFlow **and** one `orchestrate` DataJob | `Taskflow` / `Taskflow Orchestration` |
| Mapping Task  | DataFlow **and** one `transform` DataJob   | `Mapping Task` / `Task Logic`         |
| Mapping       | _not emitted_ — see notes                  | —                                     |
| Mapplet       | _not emitted_ — see notes                  | —                                     |
| Source/target | Dataset (upstream/downstream lineage)      | —                                     |

Mapping Tasks are the runnable schedules in IDMC, and that's what we emit as
first-class entities. Each MT's inner `transform` DataJob carries the
`dataJobInputOutput` aspect with the source/target tables resolved from the
Mapping it references — so cross-source lineage lands on the thing users
actually schedule and operate.

**Mappings without a Mapping Task are not emitted** (they're not runnable on
their own). **Mapplets are not emitted either** — they're internal sub-mappings
included in other mappings. The referenced Mapping's friendly name, v2 id,
and v3 GUID are still surfaced as `customProperties.mappingName` /
`mappingId` / `mappingV3Id` on every MT so you can cross-reference back to
IDMC without leaving DataHub.

#### Taskflow step DAG

The Taskflow step order is resolved from the v3 Export API (`.TASKFLOW.xml`),
parsed from the IDMC `taskflowModel` `<eventContainer>` / `<service>` /
`<link>` graph. All Taskflow GUIDs for a single ingestion run are submitted
as **one** export job for efficiency.

Rather than emitting a separate DataJob per step, the connector collapses
step references into the MT they run and chains the MT `transform` DataJobs
directly via `dataJobInputOutput.inputDatajobs`. A single `orchestrate`
DataJob is emitted per Taskflow and anchored at the end of the chain:
`inputDatajobs = [last MT]`, `outputDatasets` mirrors the last MT's outputs.

The resulting Taskflow lineage reads cleanly end to end:

```
input_dataset → MT1.transform → MT2.transform → … → MTn.transform → orchestrate → output_dataset
```

Non-data steps (command / decision / notification / …) don't participate in
the chain but are summarized in `customProperties.stepSummary` on the
orchestrate DataJob for auditing.

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
