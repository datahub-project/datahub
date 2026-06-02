### Limitations

- **Failures only (Phase 1):** Monte Carlo's API does not expose a clean per-run "pass" stream, so the connector emits only `FAILURE` run events (from alerts/incidents). Periodic `SUCCESS` events are not synthesized.
- **MCON resolution:** Each monitored asset requires one `getTable` call to resolve its MCON to a warehouse table (results are cached per MCON). Assets whose warehouse connection type is not in `connection_to_platform_map` (and not auto-mappable) are skipped with a warning.
- **Assertion typing:** All monitors and rules are modeled as `CUSTOM` assertions, matching the established connector pattern. The Monte Carlo native type is preserved in `customProperties` rather than coerced into DataHub's typed freshness/volume/SQL/field assertion schemas.
- **Monte Carlo Cloud only:** Requires a Monte Carlo Cloud account and API key pair.
