# GraphQL Shape Logging Runbook

## Overview

GraphQL shape logging provides visibility into query and response complexity through structured logging and Micrometer metrics. This runbook covers interpretation, troubleshooting, and threshold tuning.

## Metrics Dashboard

1. **graphql.shape.requests.total** — Counter of GraphQL requests by shape

   - **Dimensions**: `top_level_fields` (bounded), `operation_type` (query/mutation/subscription)
   - **Use case**: Identify which root fields drive load; detect new query patterns
   - **Typical**: 50–500 distinct `top_level_fields` combinations in production

2. **graphql.shape.field_count** — Summary of total field selections per request

   - **Metrics**: count, mean, max, p95, p99
   - **Use case**: Detect query complexity trends; identify outliers
   - **Typical**: mean 20–50 fields/request, p99 < 500 fields

3. **graphql.shape.max_depth** — Summary of maximum nesting depth per request
   - **Metrics**: count, mean, max, p95, p99
   - **Use case**: Detect deeply nested queries; identify federation complexity
   - **Typical**: mean 3–5 levels, p99 < 15 levels

## Shape Log Format

Structured logs are emitted to `com.datahub.graphql.shape` logger (inherits `ASYNC_GRAPHQL_DEBUG_FILE` appender) when thresholds are crossed:

```json
{
  "operation": "searchDatasets",
  "queryShape": "{search(input:...) {results {entity {... on Dataset {name ...}}}}}",
  "queryShapeHash": "a1b2c3d4",
  "fieldCount": 125,
  "maxDepth": 8,
  "durationMs": 3500,
  "responseFieldCount": 850,
  "maxArraySize": 100,
  "responseShape": "{results[100] {entity {name_ urn_ ...}}}",
  "errorCount": 0,
  "thresholdsCrossed": ["field_count", "duration"],
  "timestamp": "2026-04-01T12:34:56.789Z"
}
```

**Key fields:**

- **queryShape**: Normalized GraphQL shape (stripped of values/aliases), capped at 4096 chars
- **queryShapeHash**: CRC32 hash for deduplication; collision rate ~11% at 1M distinct shapes
- **fieldCount**: Total field selections in the query
- **maxDepth**: Maximum nesting depth (0-based)
- **durationMs**: Request latency
- **responseFieldCount**: Estimated total leaf values in response (based on sampling)
- **maxArraySize**: Largest array in response (heavy field tracking)
- **thresholdsCrossed**: Which thresholds triggered this log entry

## Threshold Configuration

Configure via environment variables or `application.yaml`:

```yaml
graphQL:
  shapeLogging:
    enabled: true
    fieldCountThreshold: 100 # Query field selections
    durationThresholdMs: 3000 # Request latency
    responseSizeThresholdBytes: 1048576 # ~20K fields × 50 bytes/field
    errorCountThreshold: 1 # Any errors in response
```

### Default Thresholds

| Threshold                  | Default | Dev Tuning | Prod Tuning | Rationale                                               |
| -------------------------- | ------- | ---------- | ----------- | ------------------------------------------------------- |
| fieldCountThreshold        | 100     | 50         | 200+        | Detect complex queries; dev stricter for dev/test       |
| durationThresholdMs        | 3000    | 1000       | 5000+       | Alert on slow queries; prod allows slower queries       |
| responseSizeThresholdBytes | 1048576 | 524288     | 2097152+    | 1MB ≈ 20K fields; dev stricter                          |
| errorCountThreshold        | 1       | 1          | 1           | Always alert on errors; field errors affect query plans |

### Tuning Strategy

**For Development/Staging:**

- Lower thresholds (50 fields, 1s duration, 500KB response)
- Catch inefficient queries before they reach production
- Developers can iterate quickly based on shape logs

**For Production:**

- Higher thresholds (200+ fields, 5+ sec duration, 2MB response)
- Focus on genuinely problematic outliers
- Balance visibility with log volume (too many logs = noise)

**Adaptive tuning:**

1. Enable with default thresholds
2. Monitor `thresholdsCrossed` distribution for 1 week
3. If >5% of requests cross thresholds → raise thresholds
4. If <0.1% cross thresholds → lower thresholds

## Interpreting Shape Logs

### High Field Count (100+)

**What it means**: Query selects many fields across entities.

**Examples:**

- `search` selecting 100+ fields across result unions (Dataset, Dashboard, Chart, etc.)
- Entity detail view loading all optional fields upfront

**Action:**

- Review query structure; consider field pagination or aliases
- Check if client needs all fields or if GraphQL can trim unneeded ones
- Check for N+1 federation lookups (e.g., owner details repeated across 100 results)

### High Depth (8+)

**What it means**: Deeply nested selections; may indicate federation or complex relationships.

**Examples:**

- `/dataset/{id}/owner/manager/team/organization` (5+ levels)
- Entity selectors with inline fragments across multiple types

**Action:**

- Verify federation depth is intentional (expected in multi-service DataHub)
- Check for redundant nesting (e.g., selecting owner.owner.owner)
- Consider breaking into separate queries

### Long Duration (3+ sec)

**What it means**: Query execution is slow; may indicate resolver complexity or data volume.

**Examples:**

- Large response size (1M+ fields)
- Complex filter evaluation
- N+1 resolver calls on large result sets

**Action:**

- Cross-reference with `responseFieldCount` and `maxArraySize`
- Check if response size is the bottleneck (large arrays = more data to serialize)
- Profile resolver execution; add caching if safe

### Large Response (1MB+)

**What it means**: Response contains many values; memory/serialization overhead.

**Examples:**

- 100+ search results, each with 10+ fields = 1000+ leaf values × 50 bytes = 50KB (rough estimate)
- Large array in top-level response (e.g., 500 entities × 2000 bytes each = 1MB)

**Action:**

- Review `maxArraySize`; if >100 elements, consider pagination
- Check if all selected fields are necessary
- Consider streaming or field-level pagination

### Error Count > 0

**What it means**: Query completed but with errors; field resolution failures.

**Examples:**

- Missing data in a nested entity (e.g., owner URN not found)
- Resolver timeout or partial failure
- Invalid filter causing data loss

**Action:**

- Prioritize; errors affect query plan caching
- Check error logs for resolver-specific messages
- Review error rate trend over time

## Troubleshooting

### No shape logs appearing

**Likely cause**: Thresholds set too high or logging disabled.

**Debug:**

1. Check `graphQL.shapeLogging.enabled = true` in config
2. Verify threshold values: `echo $GRAPHQL_SHAPE_DURATION_THRESHOLD_MS`
3. Lower thresholds temporarily to force logs: `GRAPHQL_SHAPE_DURATION_THRESHOLD_MS=1`
4. Check log appender is configured: grep `ASYNC_GRAPHQL_DEBUG_FILE` in logback.xml

### Shape hash collisions

**Symptom**: Multiple distinct queries have the same `queryShapeHash`.

**Cause**: CRC32 collision (expected at scale; ~11% collision rate at 1M shapes).

**Action**:

- Acceptable for metrics aggregation (collisions just reduce cardinality)
- If hash collisions become problematic (>10% of top shapes), upgrade to SHA-256:
  - Update `QueryShapeAnalyzer.crc32Hex()` → SHA-256
  - Version the hash format in the JSON (`shapeHashVersion: "sha256"`)

### Response size estimates too high/low

**Cause**: `BYTES_PER_FIELD_ESTIMATE = 50` is order-of-magnitude; actual varies.

**Real ranges:**

- Scalar fields (id, name, status): 30–40 bytes + JSON overhead
- Object references (URN, URLs): 50–80 bytes
- JSON structure (commas, spaces): 10–20 bytes per field
- Median: 40–60 bytes/field (50 is reasonable estimate)

**If estimates are consistently off by 2x:**

- Adjust `BYTES_PER_FIELD_ESTIMATE` in `GraphQLShapeConstants`
- Recompile and redeploy
- Note: This is only used for threshold alerting, not exact capacity planning

## Alerts

### Recommended Grafana Alerts

Create alerts based on these conditions:

**Alert 1: Field count spike**

```
graphql.shape.field_count{quantile="0.99"} > 300 for 5m
```

→ P99 queries suddenly much larger; may indicate query regression

**Alert 2: Response size spike**

```
increase(graphql.shape.requests.total{thresholdsCrossed=~"response_size"}[5m]) > 10
```

→ Multiple requests crossing size threshold; may indicate data explosion

**Alert 3: Error rate in shapes**

```
increase(graphql.shape.requests.total{thresholdsCrossed=~"error_count"}[5m]) / increase(graphql.shape.requests.total[5m]) > 0.05
```

→ >5% of logged requests have errors; investigate resolver failures

**Alert 4: Anomalous depth**

```
graphql.shape.max_depth{quantile="0.95"} > 20 for 10m
```

→ P95 nesting depth unusually high; may indicate federation issues

## Performance Impact

Shape logging has minimal overhead:

- **Query analysis** (hot-path): ~1-2% CPU (field selection parsing, CRC32 hash)
- **Response analysis** (tail-case): ~5-10% CPU when threshold crossed (sampling, shape building)
- **Memory**: PriorityQueue for top 10 heavy fields = ~400 bytes per response
- **Log I/O**: Async appender (ASYNC_GRAPHQL_DEBUG_FILE) decouples logging from request path

Threshold selection affects volume:

- Conservative (duration >5s): 0.1–1% of requests logged
- Moderate (duration >1s): 1–5% of requests logged
- Aggressive (duration >100ms): 10–30% of requests logged

## References

- **Configuration**: `metadata-service/configuration/src/main/resources/application.yaml` (`graphQL.shapeLogging`)
- **Code**: `metadata-io/src/main/java/com/linkedin/metadata/system_telemetry/`
  - `QueryShapeAnalyzer.java` — Query shape extraction and hashing
  - `ResponseShapeAnalyzer.java` — Response shape analysis and sampling
  - `GraphQLTimingInstrumentation.java` — Metric emission and threshold evaluation

## FAQ

**Q: Why CRC32 instead of SHA-256?**
A: Performance (CRC32 is ~10x faster). Collisions acceptable for metrics aggregation. Can migrate to SHA-256 at scale.

**Q: Can I disable shape logging for specific queries?**
A: Not currently. Filter in Grafana or post-process logs if needed.

**Q: What's the difference between field count and response field count?**
A: `fieldCount` = selections in query (e.g., `{a b c}` = 3). `responseFieldCount` = leaf values in response (e.g., array of 100 with 3 fields each = 300).
