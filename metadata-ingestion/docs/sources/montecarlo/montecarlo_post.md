### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features.

#### Assertion modeling

Every Monte Carlo monitor and custom SQL rule is modeled as a `CUSTOM` assertion, and its native
`comparisons` data is mapped onto DataHub's structured `CustomAssertionInfo` fields so Monte Carlo
assertions render through the same shared description component as dbt and Great Expectations.

For each monitor/rule, the first `comparisons` entry drives the structured fields:

- **`scope`** — `DATASET_COLUMN` when the comparison carries a column reference (`field`/`fields`),
  else `DATASET_ROWS` (table-level and row-predicate checks). This mirrors dbt's column-vs-row
  heuristic.
- **`operator`** — the MC comparison operator mapped to `AssertionStdOperator` (`EQ`→`EQUAL_TO`,
  `GT`/`GTE`/`LT`/`LTE`/`NEQ`, `INSIDE_RANGE`→`BETWEEN`, `IS_NULL`→`NULL`, `IS_NOT_NULL`→`NOT_NULL`).
  Operators with no clean DataHub equivalent (`AUTO*`, `NOOP`, `OUTSIDE_RANGE`) fall back to
  `_NATIVE_`.
- **`aggregation`** — the MC metric mapped to `AssertionStdAggregation` (`row_count`→`ROW_COUNT`,
  `distinct_count`→`UNIQUE_COUNT`, `null_count`→`NULL_COUNT`, `null_rate`→`NULL_PROPORTION`,
  `min`/`max`/`mean`/`median`/`stddev`/`sum`). Unmapped metrics fall back to `_NATIVE_`.
- **`fields`** — schema-field URNs built from the comparison's `field`/`fields`.
- **`parameters`** — thresholds mapped to `AssertionStdParameters` (`BETWEEN`→`minValue`/`maxValue`;
  scalar comparisons → `value`).

The Monte Carlo native type is preserved in `nativeType`, and native fields (severity, data-quality
dimension, resource id, comparison type, metric) are carried in `nativeParameters`. `customProperties`
keeps only the DataHub-internal `mc_monitor_uuid` correlation key. For custom SQL rules, the raw
SQL expression is captured in `customAssertion.logic`.

Monte Carlo rules can carry several independent comparisons, but DataHub's assertion model is
single-comparison. The connector maps `comparisons[0]` onto the structured fields above and folds
any remaining comparisons into `customAssertion.logic` as JSON, so a compound rule is still fully
represented. This preserves the one-monitor → one-assertion-URN scheme, so alert and run-event
wiring is unchanged.

Monitors for which no `comparisons` are returned (or the comparisons are malformed) fall back to
`scope = DATASET_ROWS` with `_NATIVE_` operator/aggregation, so they still render through the
shared path and carry their native fields on `nativeType`/`nativeParameters`.

#### Alert and incident ingestion

Monte Carlo alerts and incidents are ingested as `AssertionRunEvent` failures on their
corresponding assertion. Each event carries a timestamp, the Monte Carlo alert ID, and a link back
to the Monte Carlo UI via `externalUrl`.

### Limitations

- **Failures only:** Monte Carlo's API does not expose a per-run "pass" stream, so the connector
  emits only `FAILURE` run events (from alerts/incidents). Periodic `SUCCESS` events are not
  synthesized.
- **MCON resolution:** Each monitored asset requires one `getTable` call to resolve its MCON to a
  warehouse table (results are cached per MCON). Assets whose warehouse connection type is not in
  `connection_to_platform_map` (and not auto-mappable) are skipped with a warning.
- **Assertion typing:** All monitors and rules are modeled as `CUSTOM` assertions. Their native
  `comparisons` data is mapped onto DataHub's structured `CustomAssertionInfo` fields
  (`scope`/`operator`/`aggregation`/`fields`/`parameters`) and the native type/parameters are
  carried on `nativeType`/`nativeParameters`, but the assertions are not coerced into DataHub's
  typed freshness/volume/SQL/field assertion schemas.
- **Monte Carlo Cloud only:** Requires a Monte Carlo Cloud account and API key pair. Self-hosted
  deployments are not supported.

### Troubleshooting

#### Monitored assets are skipped with a warning

If you see warnings like `Could not resolve MCON to a DataHub dataset URN`, the warehouse
connection type for that asset is not covered by `connection_to_platform_map`. Add a mapping entry
for the connection name shown in the warning.

#### Assertion URNs do not match your warehouse source

Assertion URNs are derived from the dataset URN resolved via `connection_to_platform_map`. If the
`platform`, `platform_instance`, or `env` values differ from those used by your warehouse source
connector, the assertions will not appear on the correct dataset. Align the values in
`connection_to_platform_map` with the config of your warehouse source.

#### No assertions appear after ingestion

Verify that:

1. The API key has read access to monitors, custom rules, and alerts in the Monte Carlo UI.
2. At least one monitor is active and has fired an alert (the connector ingests only monitors that
   have associated assets and alerts).
3. The `connection_to_platform_map` covers the warehouse connections used by your monitored assets.
