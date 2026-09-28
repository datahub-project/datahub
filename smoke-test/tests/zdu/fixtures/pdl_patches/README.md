# ZDU PDL Test-Fixture Patch

The ZDU end-to-end test framework needs three pieces of code that should
**never** ship to production:

1. **`Embed.pdl` / `GlobalTags.pdl` schema bumps** (`schemaVersion=4` / `=2`)
   — needed because `SystemMetadataUtils.setSchemaVersion` reads
   `@Aspect.schemaVersion` to decide which version to stamp on writes.
   Without the bump, every write stamps v1 and the migration sweep has
   nothing to do.
2. **Four fake aspect-migration mutators** in
   `entity-registry/.../testfixtures/` (`EmbedV1ToV2`, `EmbedV2ToV3`,
   `EmbedV3ToV4`, `GlobalTagsV1ToV2`) — they stamp identifying markers
   into the aspect data so the test framework can prove the chain
   actually fired (not just bumped the version).
3. **`ZduTestMutatorConfiguration`** — Spring `@Configuration` gated on
   `zdu.test.framework.enabled=true` that wires the 4 mutator beans into
   the chain.

All three live in `test-fixtures.patch`. Production deploys never see
them.

## Apply (CI workflow does this automatically)

```bash
# From repo root
git apply --check smoke-test/tests/zdu/fixtures/pdl-patches/test-fixtures.patch
git apply        smoke-test/tests/zdu/fixtures/pdl-patches/test-fixtures.patch

# Regenerate codegen — must run AFTER apply, otherwise the build sees
# the un-bumped PDL and the mutators reference fields the wrong way.
./gradlew :metadata-models:build

# Run the ZDU E2E
cd smoke-test
DATAHUB_LOCAL_COMMON_ENV=zdu-test.env venv/bin/python -m tests.zdu
```

## Revert after a local run

```bash
# Drop the patched changes and return to a clean tree
git checkout HEAD -- \
  metadata-models/src/main/pegasus/com/linkedin/common/Embed.pdl \
  metadata-models/src/main/pegasus/com/linkedin/common/GlobalTags.pdl

rm -f \
  entity-registry/src/main/java/com/linkedin/metadata/aspect/hooks/testfixtures/EmbedV1ToV2Mutator.java \
  entity-registry/src/main/java/com/linkedin/metadata/aspect/hooks/testfixtures/EmbedV2ToV3Mutator.java \
  entity-registry/src/main/java/com/linkedin/metadata/aspect/hooks/testfixtures/EmbedV3ToV4Mutator.java \
  entity-registry/src/main/java/com/linkedin/metadata/aspect/hooks/testfixtures/GlobalTagsV1ToV2Mutator.java \
  metadata-service/factories/src/main/java/com/linkedin/gms/factory/plugins/ZduTestMutatorConfiguration.java
```

## Regenerating the patch after master drifts

If `git apply --check` starts failing on the daily CI run because
someone modified `Embed.pdl` / `GlobalTags.pdl` on master, the patch
needs to be rebased onto the new baseline:

```bash
# 1. Apply the existing patch to the new master baseline (3-way merge
#    will succeed for unrelated edits; conflicts only on actual overlap).
git apply --3way smoke-test/tests/zdu/fixtures/pdl-patches/test-fixtures.patch

# 2. Resolve any conflicts in the patched files.

# 3. Regenerate the patch from the resolved state.
git diff HEAD -- \
  metadata-models/src/main/pegasus/com/linkedin/common/Embed.pdl \
  metadata-models/src/main/pegasus/com/linkedin/common/GlobalTags.pdl \
  entity-registry/src/main/java/com/linkedin/metadata/aspect/hooks/testfixtures/ \
  metadata-service/factories/src/main/java/com/linkedin/gms/factory/plugins/ZduTestMutatorConfiguration.java \
  > smoke-test/tests/zdu/fixtures/pdl-patches/test-fixtures.patch

# 4. Revert the working-tree changes (the patch file is now the source
#    of truth again).
git checkout HEAD -- <same files as the revert block above>
```

## Why a patch and not a long-lived test branch?

A long-lived branch would have to be rebased onto master continuously to
stay buildable. A patch lets the test fixtures live alongside everything
else on master while only materializing inside the test workflow. The
freshness of the patch is verified on every CI run (`git apply --check`
fails loudly if it no longer cleanly applies), so drift is caught fast.
