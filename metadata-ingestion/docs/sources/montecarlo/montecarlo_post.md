### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features.

#### Assertion modeling

Every Monte Carlo monitor and custom SQL rule is modeled as a `CUSTOM` assertion. The Monte Carlo
native type (e.g. `freshness`, `volume`, `field_quality`, `custom_sql`) is preserved in
`customProperties` alongside the resource identifier and data-quality dimension, so no information
is lost. For custom SQL rules, the raw SQL expression is captured in `customAssertion.logic`.

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
- **Assertion typing:** All monitors and rules are modeled as `CUSTOM` assertions. The Monte Carlo
  native type is preserved in `customProperties` rather than coerced into DataHub's typed
  freshness/volume/SQL/field assertion schemas.
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
