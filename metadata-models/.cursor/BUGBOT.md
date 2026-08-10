# Metadata model (PDL) Bugbot rules

If a PDL aspect field is removed or renamed, then:
- Blocking unless accompanied by deprecation + migration notes.
- Title: "Deprecate PDL field instead of deleting"
- Body: "Stored aspects may still carry the old field. Mark deprecated and keep
  readers tolerant; document in updating-datahub.md if breaking."

If a new aspect field duplicates the entity URN key / id (e.g. `qualifiedName`
mirroring the key), then:
- Flag consistency risk. Prefer deriving from the URN or adding validation that
  keeps the fields in sync (same class of bug as structured properties).

If GraphQL/OpenAPI exposes a new model field, then:
- Check resolvers, mappers, and search mappings for follow-through — not just the
  `.pdl` edit.
