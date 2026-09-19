# Aspect Migration Mutator — Developer Guide

**Who this is for**: Any developer making a schema change to a `.pdl` aspect file that requires
migrating existing stored data to the new shape.

---

## When do you need a mutator?

| PDL Change                                        | Mutator needed?          |
| ------------------------------------------------- | ------------------------ |
| Add optional field, callers tolerate null         | No                       |
| Add optional field, back-fill from another aspect | **Yes**                  |
| Add required field                                | **Yes**                  |
| Rename or restructure a field                     | **Yes**                  |
| Remove a field                                    | **Yes** (scrub old data) |
| Add a new aspect (no existing data)               | No                       |

---

## End-to-end checklist

### Step 1 — Bump the aspect schema version in the PDL file

Open `metadata-models/src/main/pegasus/com/linkedin/<namespace>/<YourAspect>.pdl`.

Add or increment the `"schemaVersion"` field inside the `@Aspect` annotation:

```pdl
@Aspect = {
  "name": "globalTags",
  "schemaVersion": 2    // ← increment from previous value (default was 1 if absent)
}
record GlobalTags {
  ...
  newField: optional string   // ← your schema change
}
```

> **Field name is `"schemaVersion"`, not `"version"`.**
> This is read by `AspectAnnotation.java` via the constant `SCHEMA_VERSION_FIELD = "schemaVersion"`.
> The source version is whatever was there before (absent = 1).
>
> **Note**: As of writing, no production `.pdl` file yet uses `"schemaVersion"` in its `@Aspect`
> annotation — all existing aspects are implicitly at v1. You will be the first to add this field.
> The parsing logic in `AspectAnnotation.java` is correct, but verify your PDL compiles and the
> generated `AspectSpec.getSchemaVersion()` returns the value you set before wiring up the mutator.

---

### Step 2 — Create the Mutator class

Create a new file in:

```
entity-registry/src/main/java/com/linkedin/metadata/aspect/hooks/
```

Naming convention: `<AspectName>V<N>ToV<M>Mutator.java`
Example: `GlobalTagsV1ToV2Mutator.java`

#### Minimal template (no cross-aspect lookup)

```java
@Slf4j
public class YourAspectV1ToV2Mutator extends AspectMigrationMutator {

  @Override
  public String getAspectName() {
    return "yourAspectName";           // must match PDL @Aspect.name exactly
  }

  @Override
  public long getSourceVersion() {
    return 1L;                         // version of data already in the database
  }

  @Override
  public long getTargetVersion() {
    return 2L;                         // must equal sourceVersion + 1
  }

  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext ctx) {

    YourAspect old = new YourAspect(sourceAspect.data());
    YourAspect migrated = new YourAspect();

    // TODO: carry forward all existing fields
    migrated.setExistingField(old.getExistingField());

    // TODO: populate the new field
    migrated.setNewField("defaultValue");

    return migrated;   // NEVER return null — always return a v2 object
  }
}
```

#### Template with cross-aspect lookup (RetrieverContext)

Use this when the migration needs data from **another aspect of the same entity**
or from **a referenced entity's aspect** (URN embedded in the payload).

```java
@Slf4j
public class YourAspectV1ToV2Mutator extends AspectMigrationMutator {

  private static final String YOUR_ASPECT = "yourAspect";
  private static final String OTHER_ASPECT = "otherAspect";   // aspect to look up

  @Override public String getAspectName()   { return YOUR_ASPECT; }
  @Override public long   getSourceVersion() { return 1L; }
  @Override public long   getTargetVersion() { return 2L; }

  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext ctx) {

    YourAspect old = new YourAspect(sourceAspect.data());

    // ── Cross-aspect lookup using URNs embedded in the payload ─────────────
    // IMPORTANT: transform() has no entity URN. You can only look up URNs
    // that are already present inside the aspect data itself.
    // If you need the entity's own URN, override readMutation()/writeMutation() instead.

    Set<Urn> referencedUrns = old.getItems().stream()
        .map(Item::getReferencedUrn)    // URN embedded inside the aspect
        .collect(Collectors.toSet());

    // Batch fetch — one DB call for all URNs
    Map<Urn, Map<String, Aspect>> fetched =
        ctx.getAspectRetriever()
           .getLatestAspectObjects(referencedUrns, Set.of(OTHER_ASPECT));

    // ── Build migrated aspect ──────────────────────────────────────────────
    ItemArray newItems = new ItemArray();
    for (Item item : old.getItems()) {
      Item newItem = buildCopy(item);

      Aspect otherAspect = fetched
          .getOrDefault(item.getReferencedUrn(), Map.of())
          .get(OTHER_ASPECT);

      if (otherAspect != null) {
        OtherAspectRecord other = new OtherAspectRecord(otherAspect.data());
        newItem.setDenormalizedField(other.getSourceField());  // back-fill
      }
      newItems.add(newItem);
    }

    YourAspect migrated = new YourAspect();
    migrated.setItems(newItems);
    return migrated;
  }
}
```

#### Key rules for `transform()`

| Rule                                  | Reason                                                                                   |
| ------------------------------------- | ---------------------------------------------------------------------------------------- |
| **Never return `null`**               | Returning null leaves `schemaVersion` at v1; the mutator re-fires on every read forever  |
| **Always return a new object**        | Never mutate `sourceAspect.data()` directly; the base class handles in-place replacement |
| **Carry forward ALL existing fields** | Missing a field silently drops data                                                      |
| **Batch DB calls**                    | Never call `getLatestAspectObject()` inside a loop — collect all URNs first, call once   |
| **Handle missing aspects gracefully** | `getLatestAspectObjects` may return empty for some URNs — always use `getOrDefault`      |

---

### Step 3 — Register as a Spring bean

Open:

```
metadata-service/factories/src/main/java/com/linkedin/gms/factory/plugins/SpringStandardPluginConfiguration.java
```

Add a `@Bean` method:

```java
@Bean
public YourAspectV1ToV2Mutator yourAspectV1ToV2Mutator() {
  return new YourAspectV1ToV2Mutator();
}
```

The `AspectMigrationMutatorChain` bean collects all `AspectMigrationMutator` beans
automatically — you only need to declare the bean here.

> **Return type matters**: Declare the `@Bean` return type as the concrete mutator class or as
> `AspectMigrationMutator`. Do NOT return `MutationHook` — `AspectMigrationMutator` does not
> extend `MutationHook`. Returning `MutationHook` would bypass the chain and register it as a
> standalone hook, which is incorrect.

> **Feature flag**: The chain is gated behind the `zduStage20` feature flag
> (`featureFlags.zduStage20=true`). The bean must still be declared. When the flag is `false`,
> the chain is constructed with an **empty mutator list** and immediately self-disables at Spring
> startup — it does not re-evaluate the flag at runtime per request. Toggling the flag requires
> a process restart to take effect.

> **Lifecycle — disabling the chain**: The chain is designed to be disabled once all existing
> stored data has been migrated by the background sweep job. At that point call
> `chain.disable()` — after which the chain becomes a zero-overhead pass-through.
> Until `disable()` is called the chain runs on every read and write for the registered aspects.

---

### Step 4 — Write the test class

Create a new file in:

```
entity-registry/src/test/java/com/linkedin/metadata/aspect/hooks/
```

Naming convention: `<AspectName>V<N>ToV<M>MutatorTest.java`

Extend `AspectMigrationMutatorBaseTest`. You get these tests for free:

| Inherited test                                                | What it checks                        |
| ------------------------------------------------------------- | ------------------------------------- |
| `contract_targetVersionIsSourceVersionPlusOne`                | Version hop is exactly +1             |
| `contract_sourceVersionAtLeastDefault`                        | Source version ≥ 1                    |
| `writeMutation_atSourceVersion_mutatesAndBumpsVersion`        | Write path migrates and bumps version |
| `writeMutation_atDefaultSchemaVersion_mutatesAndBumpsVersion` | Null schemaVersion treated as v1      |
| `readMutation_atSourceVersion_mutatesAndBumpsVersion`         | Read path migrates and bumps version  |

You must implement four methods and may optionally implement a fifth:

```java
public class YourAspectV1ToV2MutatorTest extends AspectMigrationMutatorBaseTest {

  // ── Required ──────────────────────────────────────────────────────────────

  @Override
  protected AspectMigrationMutator mutator() {
    return new YourAspectV1ToV2Mutator();
  }

  @Override
  protected Urn entityUrn() {
    // Return a URN whose entity type matches this aspect (dataset, chart, dashboard, etc.)
    return UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)");
  }

  @Override
  protected RecordTemplate provideSourceAspect() {
    // Build a realistic v1 aspect that exercises the main migration path.
    // This is used by all inherited happy-path tests.
    YourAspect source = new YourAspect();
    source.setExistingField("value");
    // do NOT set the new field — it should be populated by transform()
    return source;
  }

  @Override
  protected void assertTransformed(RecordTemplate result) {
    // Assert that transform() produced the correct v2 output.
    // Called automatically by all inherited happy-path tests.
    YourAspect migrated = new YourAspect(result.data());
    assertEquals(migrated.getExistingField(), "value");
    assertEquals(migrated.getNewField(), "expectedValue");
  }

  // ── Optional: configure retriever mock for cross-aspect lookups ───────────

  @Override
  protected void configureRetrieverMock(@Nonnull AspectRetriever retrieverMock) {
    // Stub the fake DB to return aspects your transform() will look up.
    // mockRetriever field is also accessible in individual @Test methods.
    OtherAspectRecord other = new OtherAspectRecord();
    other.setSourceField("denormalized value");
    Aspect otherAspect = new Aspect(other.data());

    when(retrieverMock.getLatestAspectObjects(any(), eq(Set.of("otherAspect"))))
        .thenReturn(Map.of(SOME_URN, Map.of("otherAspect", otherAspect)));
  }

  // ── Edge cases you must add manually ─────────────────────────────────────

  @Test
  public void transform_allDataAlreadyMigrated_returnsV2ObjectUnchanged() {
    // Scenario: all fields already populated — no DB lookup needed.
    // Ensures: returns non-null v2 object so schemaVersion is still bumped.
  }

  @Test
  public void transform_emptyCollection_returnsEmptyV2Object() {
    // Scenario: aspect has an empty array/map — nothing to iterate.
    // Ensures: empty aspects are still bumped to v2.
  }

  @Test
  public void transform_referencedAspectMissingFromDB_gracefulFallback() {
    // Scenario: retriever returns Map.of() — referenced aspect doesn't exist.
    // Ensures: migrator doesn't throw, returns v2 with field left absent/default.
    when(mockRetriever.getLatestAspectObjects(any(), any())).thenReturn(Map.of());
    // ... assert graceful result
  }

  @Test
  public void transform_partiallyMigrated_onlyUpdatesWhatIsNeeded() {
    // Scenario: some items already have the new field, some don't.
    // Ensures: existing values are NOT overwritten; only absent ones are filled.
  }

  @Test
  public void transform_onlyFetchesUrnsNeedingLookup() {
    // Scenario: mix of items with and without the new field.
    // Ensures: batch fetch is scoped only to items that need it — stub with eq(specificSet).
    // If the mock is called with a larger set, it returns nothing and the test fails.
  }
}
```

---

## How the conditional logic is tested

The `configureRetrieverMock()` method is your control knob. By returning different data
from the fake DB, you drive the mutator down different code paths in a single `transform()`:

```
configureRetrieverMock returns...          → Branch exercised in transform()
──────────────────────────────────────────────────────────────────────
Aspect with all fields populated           → "already has value" path (no overwrite)
Aspect with fields missing                 → "back-fill from DB" path
Map.of() (nothing found)                   → "DB miss" path (graceful fallback)
Not called (don't override)                → "no lookup needed" path
```

Individual `@Test` methods can re-stub `mockRetriever` directly to override the default
set in `configureRetrieverMock()`. Mockito applies the last-registered stub first, so
per-test stubs always win.

---

## Complete file checklist

```
[ ] metadata-models/src/main/pegasus/.../YourAspect.pdl
      → set "schemaVersion": N+1 inside the @Aspect = { ... } annotation

[ ] entity-registry/src/main/java/.../hooks/YourAspectV<N>ToV<M>Mutator.java
      → extend AspectMigrationMutator
      → implement getAspectName(), getSourceVersion(), getTargetVersion(), transform()

[ ] metadata-service/factories/src/main/java/.../SpringStandardPluginConfiguration.java
      → add @Bean method for your mutator

[ ] entity-registry/src/test/java/.../hooks/YourAspectV<N>ToV<M>MutatorTest.java
      → extend AspectMigrationMutatorBaseTest
      → implement mutator(), entityUrn(), provideSourceAspect(), assertTransformed()
      → override configureRetrieverMock() if cross-aspect lookups are used
      → add edge case @Test methods (empty, all-migrated, DB miss, partial, scoped fetch)
```
