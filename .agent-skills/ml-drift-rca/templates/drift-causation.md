# Write-back templates

Fill the placeholders and write these to the catalog. Keep them terse and factual.

## 1. The `drift_causation` structured property (on the model)

A single-line, machine-and-human readable value:

```
<CHANGE_TYPE> on feature <FEATURE> (upstream <UPSTREAM_TABLE>);
<METRIC> <REFERENCE>-><CURRENT> (label-free), drop <DELTA>;
notify <OWNER_URN>; detected <ISO8601_TIMESTAMP>
```

Example:

```
null_default_regression on feature PageValues (upstream ecommerce.web_sessions);
roc_auc 0.808->0.7131 (label-free CBPE), drop 0.0949;
notify urn:li:corpGroup:data-engineering; detected 2026-07-07T22:23:47+00:00
```

`CHANGE_TYPE` is one of: `null_default_regression`, `unit_rescale` (usually benign, do not alarm), `cardinality_collapse`, `range_shift`, `schema_change`, `freshness_gap`.

## 2. The RCA document (on the model)

Plain prose, five short parts. No marketing language, no em-dashes.

```
<METRIC> for <MODEL> dropped from <REFERENCE> to <CURRENT> (<DELTA>), label-free.
The drift localizes to <FEATURE>, which shows <EVIDENCE: effect size, adjusted p-value,
range/cardinality change>. This pattern is consistent with <CHANGE_TYPE>: <one-line mechanism>.
Lineage traces <FEATURE> to <UPSTREAM_TABLE>, owned by <OWNER>, so the likely point of failure
is a recent change to that table. Recommended fix: <concrete action for the owner>.
This is a lineage-guided correlation, not a proven causal link, so confirm the upstream commit
history before rolling back.
```

## 3. The incident (on the upstream dataset)

```
type:        FRESHNESS   (or DATA_QUALITY, OPERATIONAL, as fits)
title:       Upstream change degraded <MODEL>
description: <FEATURE> in this table <CHANGE_TYPE>; estimated impact on <MODEL>:
             <METRIC> <REFERENCE>-><CURRENT>. Routed to <OWNER>.
```
