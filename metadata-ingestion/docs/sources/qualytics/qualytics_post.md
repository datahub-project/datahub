### Capabilities

Each Qualytics quality check becomes one assertion on the dataset it covers, with its
current verdict and its anomaly history as run events, and each container profile
becomes a dataset profile. The sections below cover what a run reports and when it
removes stale assertions.

#### Profiles

Profiles go only to datasets DataHub already has. A profile is an aspect of the
dataset, so writing one for a table your warehouse source has not ingested would
create a stub dataset holding nothing but that profile. The connector checks first and
skips those, counting them as `profiles_skipped_dataset_missing`; they arrive on the
first run after the warehouse source has ingested the table. The check needs a DataHub
connection, so a run to a file sink skips it and counts `profiles_existence_unchecked`.
Assertions are unaffected — they reference the dataset without creating it.

#### Reading the ingestion report

The counters are designed so a run that produced less than you expected explains
itself:

| Counter                                                 | Meaning                                                                                                                                                                     |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `datastores_unresolved` / `unresolved_datastores`       | Could not be mapped to a DataHub platform, so their containers were skipped. The usual cause of "nothing appeared".                                                         |
| `urns_resolved_by_path_reconstruction`                  | Object-store URNs whose name was reconstructed from the path. High and nothing appearing → use `datastore_to_platform_map`.                                                 |
| `unmapped_rule_types`                                   | Rule types newer than this connector build. Emitted as custom assertions.                                                                                                   |
| `datastores_dropped` / `containers_dropped`             | Excluded by your allow/deny patterns. Deliberate, not an error.                                                                                                             |
| `assertion_results_undated`                             | Results Qualytics reported without a usable timestamp; they cannot be placed on a timeline.                                                                                 |
| `anomalies_without_assertion`                           | Anomalies naming a check that has no assertion this run — the check has since been archived, or could not be parsed.                                                        |
| `anomaly_listings_failed`                               | Containers whose anomaly listing failed. Their assertions and current verdicts were emitted; only that run's failure history is missing.                                    |
| `profile_histograms_truncated`                          | Fields whose value distribution was trimmed to the 100 most frequent buckets.                                                                                               |
| `profiles_skipped_dataset_missing` / `datasets_missing` | Profiles withheld because DataHub has no such dataset yet. Usually tables the warehouse source has not ingested.                                                            |
| `containers_unprofiled`                                 | Containers Qualytics has never profiled, so there was no profile to emit. Normal for newly catalogued tables.                                                               |
| `items_unparseable`                                     | API objects whose payload did not match the expected shape, skipped one by one. Non-zero usually means a newer Qualytics build; the warnings and failures name each object. |
| `profiles_existence_check_failed`                       | Profiles skipped because DataHub could not be asked whether their dataset exists. Reported once, on the first failure.                                                      |
| `qualytics_version`                                     | The Qualytics build this run talked to. Worth quoting in any bug report.                                                                                                    |

#### Failures, warnings, and stale assertions

A problem that leaves the run's view of your assertions incomplete is reported as a
**failure**, and the run exits non-zero: a datastore or container listing that failed,
or a datastore, container or quality check whose payload could not be read. DataHub's
stale-entity removal does not delete anything after a run with failures, so a
transient API error never removes the assertions it hid. The next clean run catches up.

Problems that cannot affect which assertions exist — a profile, an anomaly listing, a
dataset-existence check — are **warnings**, and the run otherwise completes normally.

### Limitations

- **No schema is emitted.** The dataset belongs to your warehouse source, which owns its
  schema. Qualytics' view of it is a subset — it honours
  excluded fields and marks fields missing or masked — so emitting our copy would
  overwrite an authoritative schema with a partial one on every run. Field-level
  metadata still arrives through field profiles and the `schemaField` URNs on
  assertions.
- **Unrecognised rule types.** Qualytics adds rule types over time, and each deployment
  runs its own version. A rule type this connector build does not recognise is still
  emitted, as a custom assertion carrying the rule type and its configuration, and
  reported under `unmapped_rule_types`. Nothing is dropped; a non-empty
  `unmapped_rule_types` after a Qualytics upgrade means the connector is due an update.
- **Profiles reflect the last Qualytics profile operation**, not the current state of
  the table. A dataset Qualytics has not profiled recently shows correspondingly stale
  statistics.
- **Object-store dataset names are a reconstruction.** DataHub names an S3/GCS/ABS
  dataset after its table path, and your `path_spec` decides where that boundary sits.
  These resolutions are counted separately as `urns_resolved_by_path_reconstruction`.
- **Field histograms are capped** at 100 buckets, most frequent first. Truncations are
  counted as `profile_histograms_truncated`.
- **Not yet emitted:** tags, ownership, lineage, incidents, structured properties, and
  Qualytics-native datasets for computed containers. These are planned. Tags in
  particular need care, because `globalTags` is a full-replacement aspect — emitting
  Qualytics tags onto a dataset the warehouse source owns would wipe whatever else is
  on it, the same problem that keeps schema out of this connector. The likely answer is
  to tag the assertions, which this connector does own.

### Troubleshooting

**Ingestion succeeds but nothing appears in DataHub.** Almost always URN mismatch. In
order of likelihood:

1. Look for a `datastore_to_platform_map entry matched no datastore` warning. A mistyped
   key leaves its datastore to inference, with no platform instance.
2. Check `datastores_unresolved` in the report — those datastores were skipped.
3. Compare a resolved URN against the dataset in DataHub. `platform_instance` and `env`
   mismatches are the usual cause. Remember `platform_instance` names the _Qualytics_
   deployment; the warehouse's is `default_source_platform_instance` or the
   per-datastore value in `datastore_to_platform_map`.
4. Check casing. The URN casing must match what the warehouse source used. Unset,
   `convert_urns_to_lowercase` lowercases Snowflake datastores and leaves others alone,
   following each platform's own DataHub source.
5. Confirm the warehouse source has actually ingested those tables. This connector
   enriches existing datasets; it does not create them.
6. Confirm DataHub indexed the write. `datahub get --urn <assertion urn>` reads the
   primary store; the UI reads the search index. If `get` returns the assertion but the
   dataset's Quality tab is empty, the write landed and DataHub's MCL consumer has not
   indexed it — `GET /openapi/operations/kafka/mcl/consumer/offsets` on GMS shows its
   lag. Writes made through the UI or GraphQL index synchronously and skip that
   consumer, so a stalled consumer can hide behind a UI that otherwise looks current.

**`404` on every request, or a run that ingests nothing at all.** `base_url` is probably
missing its `/api` suffix. Run with `--test-source-connection`, which detects this and
prints the URL to use.

**Assertions appear but have no pass/fail history.** Either `emit_assertion_results` is
off, or the anomalies fall outside `assertion_results.start_time`. Widen the window.
Note the _current_ verdict is always emitted regardless of the window.

**Running alongside the Qualytics push integration.** Supported, and the two do not
overlap: the push integration owns incidents and structured properties, this connector
owns assertions, assertion results and profiles. This connector emits no incidents, so
there is nothing to switch off on either side.

**A `403` mid-run for one endpoint.** The token's user cannot see that object type.
Disable the corresponding feature (`emit_profiles`, `emit_assertion_results`) or widen the token's
team visibility.
