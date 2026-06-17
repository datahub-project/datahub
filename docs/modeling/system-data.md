# System Entity Data Access

Internal DataHub entities can hold **system-managed data** that should be hidden from and immutable for
end users by default. This is distinct from the **`SystemMetadata`** aspect (run IDs, versioning, and
other provenance stored alongside each aspect row). The `category: internal` flag in
`entity-registry.yml` does not enforce access control — GMS does not read it at runtime for
authorization. It mainly groups entities in generated metamodel documentation (and some build-time
OpenAPI schema generation).

Declare rules with **`@SystemEntity`** on the key aspect and optional **`@System`** on value aspects.
GMS enforces those rules at three points: **writes**, **API authorization**, and **storage reads**.

## Annotations

### `@SystemEntity` (key aspect)

Marks the entire entity as system-managed. All aspects inherit the entity policy unless overridden
with `@System`.

```pdl
@Aspect = { "name": "mySystemEntityKey" }
@SystemEntity = true
record MySystemEntityKey { id: string }
```

By default (`@SystemEntity = true` with no flags), the entity is fully hidden: users cannot read,
check existence of, or mutate any aspect.

Opt in to policy-governed access (each still requires matching DataHub policies when enabled):

```pdl
@SystemEntity = { "allowRead": true, "allowExists": true }
```

| Flag          | Effect                                                                                               |
| ------------- | ---------------------------------------------------------------------------------------------------- |
| `allowRead`   | READ requests may proceed to the policy engine instead of being denied up front                      |
| `allowExists` | EXISTS requests at the **entity** level may proceed to the policy engine when `allowRead` is not set |

**Read implies exist:** when `allowRead` is set, EXISTS eligibility is granted automatically at the
entity and aspect level. You do not need to set `allowExists` unless you want existence checks
without read access.

### `@System` (aspect override — system entities only)

Overrides `allowRead` / `allowExists` for a single aspect on a **system entity**. Registry build
**fails** if `@System` appears on an aspect whose entity is not a system entity.

```pdl
@System = { "allowRead": true, "allowExists": true }
```

Use `@System` when one aspect on a system entity should be read- or exists-eligible while others stay
hidden. The same **read implies exist** rule applies to aspect-level flags.

Aspect-level flags apply at the **storage** layer for that aspect. API-level READ still requires
entity-level `allowRead` on `@SystemEntity`. Entity-level EXISTS (URN-only checks) requires
entity-level `allowExists`, or entity-level `allowRead` via read-implies-exist.

### `@SystemEntity` placement

`@SystemEntity` is valid only on the **key aspect** PDL. Registry build fails if it appears on a
value aspect.

## How enforcement works

Effective visibility is resolved from `@SystemEntity` on the key aspect, with per-aspect `@System`
overrides where present. Exists eligibility always includes read eligibility. That policy drives all
three enforcement points below.

### 1. Writes are blocked for users

Every metadata mutation against a system entity — create, update, upsert, patch, delete, and
restate — is rejected unless the change is attributed to the **system actor** (GMS internal
operations). DataHub policies cannot grant write access to system entities.

This check runs in the ingestion validation pipeline, not in the policy engine.

### 2. API reads and exists are gated before policies

For REST, GraphQL, and other user-facing APIs, GMS checks entity-level `@SystemEntity` flags
**before** the normal policy engine runs:

- If `allowRead` is not set, READ operations return **403 Forbidden** regardless of policies.
- If neither `allowExists` nor `allowRead` is set, entity-level EXISTS operations return **403
  Forbidden**.

When the corresponding allow flag is set (including EXISTS implied by READ), the request proceeds to
standard DataHub authorization (`GET_ENTITY`, `EXISTS_ENTITY`, and related privileges). If
authorization fails there, the request is denied even though the system-data gate passed.

When `authorization.systemDataAccessControl.enabled` is `false`, this gate is skipped (along with
the write validator and storage read filter).

### 3. Storage reads filter hidden aspects

Aspect data loaded from the database is filtered using the same annotation flags. This layer does
**not** evaluate DataHub policies — it only enforces `@SystemEntity` / `@System` eligibility (with
read implies exist):

- **READ** — aspects that are not read-eligible are omitted from results as if they do not exist.
- **EXISTS** — aspect-level existence checks use per-aspect eligibility; read-eligible aspects are
  also exists-eligible.

This applies across code paths that load aspects from storage (direct reads, batch fetches,
existence checks with an explicit aspect name, and so on).

Internal GMS operations running with **system authentication** bypass storage read filters. They still
require the **system actor** for writes.

### Summary

| Operation                         | Fully hidden (`@SystemEntity = true`) | `allowRead: true` (exists implied) | `allowExists: true` only |
| --------------------------------- | ------------------------------------- | ---------------------------------- | ------------------------ |
| **User writes**                   | Rejected                              | Rejected                           | Rejected                 |
| **User reads (API)**              | 403                                   | Policy engine                      | 403                      |
| **User exists (API, entity URN)** | 403                                   | Policy engine                      | Policy engine            |
| **Aspect data from storage**      | Hidden aspects omitted                | Read-eligible aspects only         | Exists-eligible only     |

Writes always require the system actor. Storage reads use annotation flags only, not policies.

## Authoring guide

- Use a dedicated **system entity** for GMS or platform operational state.
- Register it in `entity-registry.yml`. All registry entities are included in the Entity API (OpenAPI
  v3) regardless of `category`.
- Use `category: internal` as a documentation convention for operational entities; it does not hide
  entities from the API or enforce access by itself.
- Do not use `@System` on user-facing entities — create a system entity instead.
- Set `allowRead` when operators should fetch system data via policies; EXISTS is included
  automatically. Set `allowExists` alone only when existence checks should be allowed without read
  access.

## Configuration

Turn off all system entity access enforcement (writes, API gate, and storage filtering) with:

```yaml
authorization:
  systemDataAccessControl:
    enabled: false
```

Environment variable: `SYSTEM_DATA_ACCESS_CONTROL_ENABLED=false` (requires GMS restart).

Default: `true`.
