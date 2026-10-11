# Metadata model (PDL) Bugbot rules

If a PDL aspect field is removed or renamed, then:

- Blocking unless accompanied by deprecation + migration notes.
- Title: "Deprecate PDL field instead of deleting"
- Body: "Stored aspects may still carry the old field. Mark deprecated and keep
  readers tolerant; document in `docs/how/updating-datahub.md` if breaking."

If a new aspect field duplicates the entity URN key / id (e.g. `qualifiedName`
mirroring the key), then:

- Flag consistency risk. Prefer deriving from the URN or adding validation that
  keeps the fields in sync (same class of bug as structured properties).

If GraphQL/OpenAPI exposes a new model field, then:

- Check resolvers, mappers, and search mappings for follow-through — not just the
  `.pdl` edit.

## Timeseries fields

If timeseries aspects add fields that are intended for timeseries **aggregation**
or search/filter on the timeseries index, then:

- Flag missing `@TimeseriesField` (and related) annotations when aggregation /
  indexing requires them.
- Do not require `@TimeseriesField` on every measure or attribute in a timeseries
  aspect (canonical aspects such as `DatasetProfile` include non-annotated
  measures).
- Per-observation attributes (e.g. origin) must not live only on the definition
  when each observation needs its own value.

## Entity registry

If a PR adds a new **top-level persisted entity aspect** (or a new aspect key),
then:

- High when `entity-registry.yml` is not updated for the owning entity.
- Do not flag nested value records that happen to carry `@Aspect` (or similar)
  annotations but are not independently stored aspects.
- Title: "Register new aspect in entity-registry.yml"
