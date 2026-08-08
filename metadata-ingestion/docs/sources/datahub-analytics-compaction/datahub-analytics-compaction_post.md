### Capabilities

Use the **Important Capabilities** table above as the source of truth. This
module triggers compaction and records result counters in the source report
(`hours_sealed`, `days_compacted`, `months_compacted`, lock/skip flags).

#### Soft-skip behavior

- HTTP `503` means the compaction backend is unavailable; the run soft-skips
  without failing the pipeline.
- When another compactor holds the advisory lock, the API may return success
  with `lockNotAcquired=true`; the source records that in the report and exits
  cleanly.

#### Config overrides

Optional fields (`max_hours_to_seal`, `max_days_to_compact`,
`max_months_to_compact`, `max_wall_clock_millis`) override server defaults for
catch-up or bounded runs. Leave them unset for normal scheduled maintenance.

### Limitations

- This module is not a general external-source ingestion connector.
- Compaction only applies when PostgreSQL is the usage-events source of truth
  and a compaction service is registered in GMS.
- It does not backfill Elasticsearch historical usage indexes when switching
  sources of truth.

### Troubleshooting

- If every run soft-skips with `503`, confirm pgAnalytics is enabled and the
  compaction service bean is present in GMS.
- If `lockNotAcquired` stays true across many runs, check for a stuck compact
  caller or long-running catch-up with a large lookback.
- HTTP `500` with `failed=true` means seal/rollup hit a SQL or lock-setup error;
  the SYSTEM source fails the run so operators notice. Check GMS logs around
  `/openapi/operations/analytics/compact`.
