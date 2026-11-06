- Start Date: 2026-08-07
- RFC PR: (filled in after opening)
- Discussion Issue: (none)
- Implementation PR(s): (empty)

# Interaction certificate: recording joint upstream failure structure on ML assets

## Summary

Add an aspect that records, for an ML asset, the **interaction order** of its dependence on upstream
feeds and the **minimal failure set** that order implies: the smallest group of upstreams that must
fail *together* to move the model's metric.

## Basic example

```
InteractionCertificate (aspect of mlModel, mlFeatureTable)
  interactionOrder   : int          # 1 = additive; k = irreducibly k-way
  minimalFailureSet  : array[Urn]   # the upstreams that must be repaired together
  metric             : string       # what was measured, e.g. "holdout_accuracy"
  probe              : string       # how upstreams were degraded, e.g. "staleness_24h"
  resolutionFloor    : float        # dividends below this were treated as unresolvable
  computedAt         : timestamp
  producer           : string
```

A consumer reads it like this:

> `interactionOrder = 3` on this feature view means no single upstream, and no pair of upstreams,
> explains a failure of it. An agent that reverts one feed at a time will not find the cause.

## Motivation

Every upstream health signal DataHub carries today is **per-asset**: freshness, volume, schema
changes, per-column profiles, assertions. None of them can express a statement about assets
*jointly* — "this model degrades only if these three feeds go stale together."

That statement is not decorative. In reliability engineering it is the definition of a common-cause
failure, the case redundancy specifically does not protect against. The ML instance is ordinary: one
refresh job dies and three correlated features go stale at once. Meta's "Moving Fast With Broken
Data" (arXiv:2303.06094) reports that corrupted partitions are near-ubiquitous at scale and that
their detection system organises around *groups of correlated features* for exactly this reason.

The consequence is concrete, and it is why this belongs in the catalog rather than inside one tool.
An agent that root-causes by reverting one upstream at a time — or even every pair — **cannot**
identify a failure set larger than the coalitions it examined. This is not a limitation of its model
or prompt; the information is not present in what it measured. If the catalog records that a feature
view's interaction order is 3, any agent reading it knows *before starting* that one-at-a-time
debugging cannot succeed there, and knows how many things must be fixed together.

Today this is expressible only as ad-hoc structured properties, which works but is private to
whoever produced it.

## Requirements

- Record interaction order and minimal failure set for an ML asset.
- Carry the conditions under which the number was computed. An interaction order describes a model
  **under a specific perturbation**, not a model. Numbers from different probes must not be silently
  compared.
- Record the resolution floor applied, so a consumer can tell a real interaction from a metric
  artefact.
- Attach to both `mlModel` and `mlFeatureTable`: the model is what degrades, the feature view is what
  owns the feeds.
- Be producible by any tool, not only by the proposer's.

### Extensibility

The same shape extends to non-ML assets whose failure structure is measurable the same way, and to
richer spectra (the full dividend distribution rather than just its top order) if a consumer ever
needs more than the headline number. Keeping `probe` and `metric` as free-form strings is deliberate
so producers are not blocked on an enumeration that does not exist yet.

## Non-Requirements

- **Not a computation standard.** Different producers will probe differently. This proposes a shape
  for the result and the metadata needed to interpret it.
- **Not causal identification.** The value is counterfactual attribution under an assumed,
  catalog-given DAG plus a mechanism-independence assumption. It should not be read as an identified
  causal effect, and the aspect should not imply one.
- Not a proposal for alerting or UI behaviour, though both are natural consumers.

## Detailed design

For an ML asset with upstream feeds `P`, let `v(S)` be the metric measured with the feeds in `S`
counterfactually degraded and the rest fresh. The Harsanyi (Mobius) dividends are

```
m(S) = sum over T subset of S of (-1)^(|S| - |T|) v(T)
```

- **interaction order** = `max { |S| : |m(S)| > resolutionFloor }`
- **minimal failure set** = the support of the largest-magnitude dividend at that order

Order 1 means additive dependence, and single-feed ablation suffices. Order `k` means any method
probing coalitions smaller than `k` cannot identify the minimal failure set. That follows directly
from the decomposition; it is not a new result. What is proposed here is treating it as catalog
metadata.

Two fields carry more weight than they look like they do:

**`probe`.** The order is a property of the asset under a specific perturbation. Staleness works well
as a default because it is what actually happens in production, and because it does not move any
feed's marginal distribution at probe time — so the measurement is not confounded by ordinary drift.
A producer that changes its probe between periods is measuring its own probe, not the asset.

**`resolutionFloor`.** On a finite holdout, a metric like accuracy is quantized at `1/n_eval`. A
single flipped prediction therefore produces a non-zero top-order dividend, which inflates the
reported order to the maximum the feed set allows. Any implementation without a floor will report
impressive and meaningless orders. This is not hypothetical: it is the first thing that went wrong
when the reference implementation moved from synthetic data to a real dataset, where non-interacting
dividends are never exactly zero.

## How we teach this

The concept has a one-sentence form that lands without the maths: **"how many upstreams have to
break at the same time before this model notices."** The order is that number; the minimal failure
set is which ones.

The audience is ML platform and data reliability teams, and the agents acting for them. It fits as a
continuation of the existing ML entity model rather than a new pattern: `mlFeatureTable` already owns
the relationship between a model and its feeds, and this describes a property of that relationship.

For incident-response tooling the teaching point is a decision rule: check the order before choosing
a strategy, because below it, one-at-a-time debugging is not slow, it is incapable.

## Drawbacks

- **It requires a replayable scoring function.** Computing `v(S)` means scoring the model on
  counterfactual upstream states. Teams with a feature store have this — a point-in-time join returns
  a feed's value as of an earlier moment, which is precisely a stale feed — but teams with no offline
  replay path cannot produce the aspect at all. It will be unevenly populated.
- **Exact computation is exponential in the number of feeds.** Practical implementations prefilter to
  a small set and enumerate exactly, or sample above that. Two producers may therefore disagree, which
  is part of why `producer` is in the record.
- It adds a number that is easy to quote without its conditions. The `probe` and `resolutionFloor`
  fields mitigate this, but a number in a UI invites decontextualised comparison.

## Alternatives

- **Structured properties, which is what the reference implementation ships today.** They work and are
  queryable. What they do not give is a shape other tools can rely on, which is the entire value of
  putting this in the catalog rather than in a monitoring product.
- **Leave it in the monitoring tool.** Then every consumer that wants to act on it needs an
  integration with that specific tool, and the agent reading the catalog learns nothing.
- **Prior art.** Harsanyi dividends and interaction indices are established (Grabisch & Roubens 1999;
  Shapley-Taylor, Sundararajan et al. 2020; n-Shapley, Bordt & von Luxburg, AISTATS 2023).
  Shapley-based root-cause analysis over causal graphs is established (Budhathoki et al., ICML 2022).
  Interventional rather than conditional attribution follows Janzing et al. 2020, because correlated
  upstreams break conditional attribution — the regime this aspect exists for. What appears unclaimed
  is treating interaction order as a monitored, catalog-resident property of an asset rather than a
  one-off explanation produced during an incident.

## Rollout / Adoption Strategy

Additive and non-breaking: a new aspect that is absent until a producer writes it. Consumers must
treat absence as "unknown", never as "order 1", since the two are very different claims and the
difference is exactly what makes the aspect useful.

## Future Work

- A timeseries form, so a *rising* order becomes queryable. A model accumulating higher-order
  dependence on correlated upstreams is the latent condition a common-cause failure later exploits,
  and that trend is more actionable than any single reading.
- Surfacing the order on the asset page, and in incident-response tooling as a strategy hint.
- Deriving candidate assertion groups from the minimal failure set: those feeds are the ones worth
  monitoring as a unit.

## Unresolved questions

1. Should `minimalFailureSet` hold URNs (precise, brittle across re-ingestion) or names?
2. Should the aspect be timeseries by nature? The monitoring use case wants history; the
   incident-response use case wants only the latest.
3. Is `probe` better as a free-form string or an enumeration with an escape hatch? An enumeration
   would make comparability checkable, at the cost of blocking producers doing something unforeseen.
4. Should a consumer be able to distinguish "computed exactly" from "sampled", beyond the `producer`
   string?
