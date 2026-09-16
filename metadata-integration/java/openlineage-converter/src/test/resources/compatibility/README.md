# OpenLineage consumer compatibility corpus

74 events vendored from [OpenLineage/compatibility-tests](https://github.com/OpenLineage/compatibility-tests)
(`consumer/scenarios`), Apache-2.0. Directory names and event filenames match upstream so a refresh
is a straight copy.

These are payloads real producers emitted — Airflow, Spark on Dataproc, BigQuery — not events built
with the OpenLineage Java builders. That is the whole reason to keep them. The rest of this module's
tests assemble events in memory and never deserialize anything, so a mapping can be correct against
a builder and still find nothing in what a producer actually sends. `CompatibilityCorpusTest` runs
every event through `OpenLineageClientUtils.runEventFromJson` first.

| Scenario                                       | Events | Producer                        |
| ---------------------------------------------- | ------ | ------------------------------- |
| `airflow`                                      | 32     | Airflow OpenLineage provider    |
| `spark_dataproc_simple_producer_test_complete` | 16     | Spark on Dataproc               |
| `spark_dataproc_bigquery_shakespare`           | 12     | Spark on Dataproc, BigQuery I/O |
| `CLL`                                          | 9      | Spark, column-level lineage     |
| `spark_dataproc_simple_producer_test`          | 4      | Spark on Dataproc               |
| `simple_run_event`                             | 1      | hand-written minimal event      |

`simple_run_event` declares `"runId": "run_id"`. The spec requires a UUID and the OpenLineage client
rejects it, so it is excluded from the conversion sweep and asserted separately: it has to fail
during deserialization, which is what lets the REST endpoint answer 400 rather than 500.

Every event is a `RunEvent`. The corpus exercises nothing on the `JobEvent` / `DatasetEvent`
static-lineage path, which is covered by the hand-written fixtures beside it.

## What the corpus carries, and what we do with it

Counts are events carrying the facet. Recorded so that a facet arriving from a real producer and
being dropped is a visible decision rather than something nobody noticed.

### Run facets

| Facet                      | Events | Status                                                               |
| -------------------------- | ------ | -------------------------------------------------------------------- |
| `parent`                   | 61     | Partial — becomes a job-to-job edge; the run-to-run link is not kept |
| `processing_engine`        | 57     | Orchestrator, plus custom properties                                 |
| `spark_properties`         | 41     | Custom properties                                                    |
| `gcp_dataproc_spark`       | 32     | **Dropped**                                                          |
| `airflow`                  | 26     | Custom properties                                                    |
| `unknownSourceAttribute`   | 20     | Custom properties                                                    |
| `nominalTime`              | 16     | DataProcessInstance custom properties                                |
| `environment-properties`   | 9      | **Dropped**                                                          |
| `spark_jobDetails`         | 8      | Custom properties                                                    |
| `spark.logicalPlan`        | 7      | Custom properties                                                    |
| `externalQuery`            | 3      | DataProcessInstance custom properties                                |
| `airflowDagRun`            | 3      | **Dropped**                                                          |
| `airflowState`             | 3      | **Dropped**                                                          |
| `spark_applicationDetails` | 3      | **Dropped**                                                          |
| `spark_unknown`            | 1      | **Dropped**                                                          |

### Job facets

| Facet         | Events | Status                                                                    |
| ------------- | ------ | ------------------------------------------------------------------------- |
| `jobType`     | 73     | DataJob custom properties                                                 |
| `gcp_lineage` | 32     | **Dropped**                                                               |
| `sourceCode`  | 20     | **Dropped** — the sibling `sourceCodeLocation` is mapped; this one is not |
| `sql`         | 18     | Partial — survives only as `transformOperation` on column-level lineage   |
| `ownership`   | 16     | DataJob `Ownership`                                                       |
| `airflow`     | 3      | Custom properties                                                         |

### Dataset facets

| Facet                  | Events | Status                                                             |
| ---------------------- | ------ | ------------------------------------------------------------------ |
| `schema`               | 56     | `SchemaMetadata`                                                   |
| `dataSource`           | 49     | Not mapped, by design — the namespace carries the same information |
| `symlinks`             | 40     | URN resolution                                                     |
| `columnLineage`        | 20     | `fineGrainedLineages`                                              |
| `lifecycleStateChange` | 12     | `Operation`                                                        |
| `outputStatistics`     | 6      | `Operation`                                                        |

Non-standard vendor facets (`gcp_*`, `airflowDagRun`, `airflowState`, `spark_applicationDetails`,
`spark_unknown`) fall through the `processRunFacetEntry` switch. `environment-properties` and
`sourceCode` are the two worth revisiting: both are widely emitted and both have somewhere obvious
to land.

## Golden comparison

`CompatibilityGoldenTest` writes everything the converter emits for each event — every proposal,
URN, change type and aspect field — to `src/test/resources/compatibility-golden/`, mirroring the
scenario layout. `CompatibilityCorpusTest` says what we _meant_ by naming a few facets and the
aspects they become; the goldens say what we _produce_, including the parts nobody thought to
assert.

Regenerate after an intentional change, then read the diff:

```bash
./gradlew :metadata-integration:java:openlineage-converter:test \
    --tests "*CompatibilityGoldenTest*" -Dopenlineage.golden.regenerate=true
```

The task deliberately fails after rewriting, so a regeneration cannot be mistaken for a passing
run. A golden nobody reads is a golden that has stopped catching anything.

Edge audit stamps come from the wall clock and would differ on every run, so any timestamp within a
day of now is written as `<WALL_CLOCK>`. Timestamps the event itself supplied are left intact and
are real assertions — the corpus is from 2024, so the two never collide.

## What the converted output actually looks like

Checked by converting the corpus and reading the proposals, not only by asserting that conversion
succeeded.

Sound: dataset URNs resolve across four platforms from the namespace alone —
`bigquery,bigquery-public-data.samples.shakespeare`, `gcs,mock-bucket/result.csv`,
`file,/files/temp/data.txt`, `hive,default.t1`. The DataFlow/DataJob hierarchy matches the
producer's own — `(airflow,BQ,<cluster>)` with `BQ.upload`, `BQ.copy`, `BQ.download` beneath it —
and each run gets a DataProcessInstance keyed on `runId` whose `parentTemplate` points at its
DataJob. `dataJobInputOutput` is emitted as a patch with well-formed edges.

Worth knowing:

- 30 of the 73 events produce a DataJob with no `dataJobInputOutput` at all. They are START events
  carrying no inputs or outputs, so a run observed only at START leaves a job with no edges.
- `dataProcessInstanceInput` and `dataProcessInstanceOutput` are written as whole aspects on every
  event, including `{"inputs":[]}` for a START. The last event of a run therefore decides what the
  run entity reports, which is the run-scoped half of the incremental-accumulation gap.
- Ownership patches carry an empty trailing segment —
  `/owners/urn:li:corpuser:<owner>/TECHNICAL_OWNER//` — because the two-argument `addOwner` does not
  pass `Owner.source` through.
- The Airflow events name their owner `***`, which upstream redacted, and the converter turns it
  into `urn:li:corpuser:***` without complaint. Nothing validates an owner string before it becomes
  a URN.

## What this corpus cannot test

Every event is a `RunEvent`, so the `JobEvent` / `DatasetEvent` path gets no coverage here.

It also cannot exercise incremental accumulation. Of the 27 runs with both a START and a terminal
event, **none** has a START richer than its terminal event: 13 go from empty to populated and 14 are
identical. The loss case — a producer declaring inputs at START and only outputs at COMPLETE — does
not occur anywhere in this corpus, so the accumulation behaviour has to keep its own fixtures.
