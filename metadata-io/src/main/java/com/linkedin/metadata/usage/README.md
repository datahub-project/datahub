# Flush alignment

When `alignmentPeriodSeconds` is non-zero, flush batches are split at UTC calendar
boundaries — a drain that would cross a boundary ends the current batch at the
boundary and starts a fresh window there. Multiple batches per period are normal;
flush consumers should sum additive rows and union distinct identities per aligned
`window_start`.

The flush coordinator ticks on `scheduledIntervalSeconds` (default 60s). Each tick
flushes before the next boundary when within one interval of it, otherwise on the
normal schedule.

| `alignmentPeriodSeconds` | Grid (UTC)   |
| ------------------------ | ------------ |
| `0` (default)            | Disabled     |
| `3600`                   | Top of hour  |
| `900`                    | 15 minutes   |
| `86400`                  | UTC midnight |

Set `USAGE_AGGREGATION_ALIGNMENT_PERIOD_SECONDS` to enable alignment (e.g. `3600` for
hourly). When alignment is on, keep `scheduledIntervalSeconds` > 0 so the
coordinator can flush before each boundary. With `scheduledIntervalSeconds=0`, no
periodic ticks run — flushes rely on `maxWindowSeconds` and cardinality triggers
only, which can miss boundary timing unless `maxWindowSeconds` divides the alignment
period cleanly.
