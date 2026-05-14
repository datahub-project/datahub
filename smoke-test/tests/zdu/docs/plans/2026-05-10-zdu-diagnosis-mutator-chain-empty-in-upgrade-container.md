# Diagnosis — `AspectMigrationMutatorChain` empty in datahub-upgrade container

**Status:** RESOLVED via JAR rebuild. The instrumentation confirmed the @Bean methods do fire correctly when a fresh `:datahub-upgrade:bootJar` build is loaded. See "Resolution" section at the bottom.

**Symptom:** When `UpgradeNonBlockingPhase` (Plan 7) runs `system-update -u SystemUpdateNonBlocking` against the dev Compose stack, `MigrateAspectsStep` logs:

```
MigrateAspects: no mutators registered — nothing to migrate.
```

This causes the upgrade to no-op for the IO-pool entities, and Plan 7's live integration check hangs at `sweep_timeout_s` (601s) waiting for a `Sweep complete. Total migrated: N` line that never comes.

---

## What's been ruled out

1. **Env var propagation.** `docker compose run -e ASPECT_MIGRATION_MUTATOR_ENABLED=true system-update-debug` correctly delivers the env var to the container. Verified via `env | grep ASPECT` inside the container.
2. **Spring property binding.** Application yaml resolves `featureFlags.aspectMigrationMutatorEnabled: ${ASPECT_MIGRATION_MUTATOR_ENABLED:${ZDU_STAGE_20:false}}`. The chain init log line in the upgrade container says `aspectMigrationMutatorEnabled=true`, so the flag is bound correctly.
3. **Bytecode parity with GMS.** The `BOOT-INF/lib/factories.jar` inside `datahub-upgrade.jar` is byte-identical (md5 `ce0fc5c708e2fc628682078922643a27`) to `/tmp/gms/extraction/BOOT-INF/lib/factories.jar` in the GMS container. So the @Bean methods exist as identical bytecode in both places.
4. **@Bean annotations exist.** `javap -v -p` on the upgrade container's `SpringStandardPluginConfiguration.class` confirms `globalTagsV1ToV2Mutator()`, `embedV1ToV2Mutator()`, `embedV2ToV3Mutator()` (commented-out source but bytecode still emits the method without @Bean), `embedV3ToV4Mutator()`, and `aspectMigrationMutatorChain()` are all present. The first three have `RuntimeVisibleAnnotations: org.springframework.context.annotation.Bean`.
5. **GMS chain works.** A direct GMS read of a v1-stored `embed` aspect returns `schemaVersion=2`, proving the v1→v2 mutator fires on the GMS read path. (The chain stops at v2 because the v2→v3 hop is intentionally commented-out for ZDU SIM TC-007/TC-011 simulation.)
6. **`@ComponentScan` includes the package.** `GeneralUpgradeConfiguration` (active for `-u SystemUpdate*` upgrades) scans `com.linkedin.gms.factory`, which covers `SpringStandardPluginConfiguration` (in `com.linkedin.gms.factory.plugins`). The chain bean firing confirms the class IS loaded.
7. **No alternate chain construction.** `grep -rn "new AspectMigrationMutatorChain"` in `--include="*.java"` returns exactly one match — the one in `SpringStandardPluginConfiguration.aspectMigrationMutatorChain`.

---

## What's odd

The chain @Bean method (`aspectMigrationMutatorChain`) fires correctly in BOTH Spring contexts spawned by `UpgradeCliApplication.main` (sql-setup, then general). Both report `aspectMigrationMutatorEnabled=true, mutator(s)=0`. The autowired `List<AspectMigrationMutator> migrationMutators` parameter is empty.

Yet the @Bean methods on the same `@Configuration` class that produce `AspectMigrationMutator` instances do NOT appear in the `Enabled N plugins` list emitted by `PluginFactory:282` — and Spring should be calling them because they're plain unconditional `@Bean public AspectMigrationMutator …()` methods on a loaded `@Configuration` class.

The two log lines per run (one per Spring context) look like:

```
INFO c.l.m.a.h.AspectMigrationMutatorChain:80 - AspectMigrationMutatorChain: no mutators registered, chain disabled.
INFO c.l.g.f.p.SpringStandardPluginConfiguration:112 - Initialized AspectMigrationMutatorChain: aspectMigrationMutatorEnabled=true, mutator(s)=0
```

---

## Most likely remaining hypotheses

**H1 — `@Bean` methods are silently skipped because Spring resolves a different definition first.** Spring's `ConfigurationClassPostProcessor` may be deduplicating `SpringStandardPluginConfiguration` at class-load time and registering a _partial_ set of `@Bean` methods. This could happen if there's a packaging quirk where two copies of the class exist on the classpath (one missing the per-mutator @Beans, one having them) and Spring picks the bare copy first. The `factories.jar` md5 match argues against this for the upgrade container itself, but a different shaded copy could exist in another inner JAR. Test by dumping every `SpringStandardPluginConfiguration.class` reachable via the upgrade-cli classloader.

**H2 — Bean method invocation conflict between `@Configuration` proxy and `PluginFactory` registry.** The entity-registry `PluginFactory` has its own plugin discovery mechanism. If it's claiming ownership of the `SpringStandardPluginConfiguration` instance and bypassing Spring's CGLIB-proxied `@Bean` interception, the chain method runs but the per-mutator methods don't. Worth inspecting `PluginFactory.scan()` against the upgrade classpath.

**H3 — A different `@Configuration` exclude filter is silently dropping the @Bean methods.** Possible if `GeneralUpgradeConfiguration`'s `excludeFilters` somehow matches the `*Mutator` types via a generic ASSIGNABLE_TYPE rule. Looking at the actual exclude list (`ScheduledAnalyticsFactory`, `AuthorizerChainFactory`, etc.) none should match `AspectMigrationMutator` — but worth instrumenting to confirm.

H1 is most likely given the symptom shape.

---

## Concrete next step (single PR)

Add three log lines and rebuild the upgrade image. Then re-run the diagnostic.

**File:** `metadata-service/factories/src/main/java/com/linkedin/gms/factory/plugins/SpringStandardPluginConfiguration.java`

Modify each per-mutator @Bean method to log on construction:

```java
@Bean
public AspectMigrationMutator globalTagsV1ToV2Mutator() {
    log.info("[ZDU-DIAG] @Bean globalTagsV1ToV2Mutator instantiated by class {}", this.getClass().getName());
    return new GlobalTagsV1ToV2Mutator();
}

@Bean
public AspectMigrationMutator embedV1ToV2Mutator() {
    log.info("[ZDU-DIAG] @Bean embedV1ToV2Mutator instantiated by class {}", this.getClass().getName());
    return new EmbedV1ToV2Mutator();
}

@Bean
public AspectMigrationMutator embedV3ToV4Mutator() {
    log.info("[ZDU-DIAG] @Bean embedV3ToV4Mutator instantiated by class {}", this.getClass().getName());
    return new EmbedV3ToV4Mutator();
}
```

Also instrument the chain @Bean to dump the autowired list contents:

```java
@Bean
public AspectMigrationMutatorChain aspectMigrationMutatorChain(
    List<AspectMigrationMutator> migrationMutators) {
  log.info("[ZDU-DIAG] aspectMigrationMutatorChain received {} mutators: {}",
      migrationMutators.size(),
      migrationMutators.stream().map(m -> m.getClass().getSimpleName()).toList());
  // … existing body …
}
```

**Build + run:**

```bash
./gradlew :datahub-upgrade:dockerImage
scripts/dev/datahub-dev.sh env restart   # if needed to reload factories.jar
docker compose -f docker/profiles/docker-compose.yml run --rm \
  -e ASPECT_MIGRATION_MUTATOR_ENABLED=true \
  -e SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED=true \
  -e SYSTEM_UPDATE_MIGRATE_ASPECTS_UPGRADE_VERSION=zdu-debug-rebuild \
  system-update-debug -u SystemUpdateNonBlocking 2>&1 | grep ZDU-DIAG
```

**Expected outcomes:**

- If the per-mutator log lines fire → H1/H2 wrong; the @Bean methods _are_ called but Spring drops the resulting beans before injecting into the chain. Investigate Spring scope / `defaultListableBeanFactory` filtering. Check whether `BeanDefinitionOverrideException` is being silently swallowed.
- If the per-mutator log lines do NOT fire → H1 confirmed. The `@Configuration` is being loaded as a _bare_ class (Spring is calling the unfaithful copy or skipping `@Bean` invocation entirely). Inspect `ConfigurationClassPostProcessor` debug logs and look for a duplicate class definition on the classpath.
- The chain log line shows what `migrationMutators.size()` actually is and what classes are in it; even without the per-mutator logs, this disambiguates whether the issue is "beans never created" vs "beans created but not autowired here".

---

## Why this isn't a Plan-style commit-by-commit blueprint

This is a single 1-line-per-method instrumentation change that needs a Java rebuild + Docker image rebuild before it produces useful signal. Once the diagnostic confirms the actual root cause, a proper plan can be drafted to fix it (likely 1-2 commits in a single config file).

The instrumentation should NOT ship to production — it's a `[ZDU-DIAG]` tagged INFO log that should be removed once the root cause is identified.

---

## Workaround in the meantime

`UpgradeNonBlockingPhase` continues to be skipped in Plan 7/8 live integration checks via `ZDU_SKIP_PHASES=upgrade,upgrade_nonblocking,rolling_restart`. Suite A regression baseline (14 PASS / 7 XFAIL / 1 pre-existing TC-020 / 1 SKIP) is preserved.

Plan F-5 (just landed) ensures the IO-pool entities land at v1 in MySQL — so the moment the upgrade-container chain is non-empty, the race-window assertion will work end-to-end without further test-side changes.

---

## Recorded artifacts

- Live env dump showing `ASPECT_MIGRATION_MUTATOR_ENABLED=true` reaches the container.
- Live chain init log: `Initialized AspectMigrationMutatorChain: aspectMigrationMutatorEnabled=true, mutator(s)=0` (twice — once per Spring context).
- `javap -v -p` confirming all four `@Bean` methods exist with correct annotations in the upgrade container's `SpringStandardPluginConfiguration.class`.
- md5 hash match: `factories.jar` byte-identical between upgrade and GMS containers (`ce0fc5c708e2fc628682078922643a27`).
- GMS read-path probe: storage v1 → response `schemaVersion=2`, proving GMS chain has the v1→v2 mutator wired.
- `Enabled 28 plugins` log from `PluginFactory:282` listing `AspectMigrationMutatorChain` but no `EmbedV*Mutator` entries (expected — they're not MutationHooks).

---

## Resolution

The instrumentation was added to `metadata-service/factories/.../SpringStandardPluginConfiguration.java`:

```java
@Bean
public AspectMigrationMutatorChain aspectMigrationMutatorChain(
    List<AspectMigrationMutator> migrationMutators) {
  log.info("[ZDU-DIAG] aspectMigrationMutatorChain received {} mutator(s) from autowiring: {}",
      migrationMutators.size(),
      migrationMutators.stream().map(m -> m.getClass().getSimpleName()).collect(Collectors.toList()));
  log.info("[ZDU-DIAG] this.getClass()={} configClassLoader={}",
      this.getClass().getName(), this.getClass().getClassLoader());
  // … existing body …
}

@Bean public AspectMigrationMutator globalTagsV1ToV2Mutator() {
  log.info("[ZDU-DIAG] @Bean globalTagsV1ToV2Mutator instantiated by class={}", this.getClass().getName());
  return new GlobalTagsV1ToV2Mutator();
}
// … same for embedV1ToV2Mutator and embedV3ToV4Mutator …
```

After `./gradlew :datahub-upgrade:bootJar` (the system-update-debug compose service mounts `datahub-upgrade/build/libs/` into `/datahub/datahub-upgrade/bin/`, so the rebuilt JAR is auto-loaded), running `system-update -u SystemUpdateNonBlocking` produced:

```
[ZDU-DIAG] @Bean globalTagsV1ToV2Mutator instantiated by class=...$$SpringCGLIB$$0
[ZDU-DIAG] @Bean embedV1ToV2Mutator instantiated by class=...$$SpringCGLIB$$0
[ZDU-DIAG] @Bean embedV3ToV4Mutator instantiated by class=...$$SpringCGLIB$$0
[ZDU-DIAG] aspectMigrationMutatorChain received 3 mutator(s) from autowiring: [GlobalTagsV1ToV2Mutator, EmbedV1ToV2Mutator, EmbedV3ToV4Mutator]
Initialized AspectMigrationMutatorChain: aspectMigrationMutatorEnabled=true, mutator(s)=3
MigrateAspects initialised: 2 aspect(s), upgradeVersion=v1.5.0rc1-0, aspects={embed=4, globalTags=2}.
```

The chain now has 3 mutators. Running the framework's full Suite A pipeline (with `upgrade_nonblocking` enabled, only `upgrade,rolling_restart` skipped) produces:

```
✓ upgrade_nonblocking  passed  (11.4s)
  total_migrated=11, sweep_duration_s=1.6, io_reads=900, io_writes=5, write_failures=0
✓ runtime_migration    passed  (0.1s)
  read_probes=5, read_passed=5, write_probes=3, write_passed=3
```

**Suite A baseline preserved:** 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP — same as the skipped-phase baseline.

### Hypothesis on the original failure mode

The original "no mutators registered" symptom from Plan 7/8 era was most likely caused by a stale `datahub-upgrade.jar` in `datahub-upgrade/build/libs/` that had been compiled at an earlier point (e.g., before some upstream change to the @Bean methods or their dependencies). The `system-update-debug` compose service mounts that directory verbatim into the container, so a stale build silently kept producing the old behavior.

**Why the diagnostic instrumentation appeared to "fix" it:** the only reason this run produces a different result is that adding the log lines forced a fresh `compileJava` + `bootJar` build, which produced a JAR with up-to-date dependencies and bean wiring. The instrumentation itself wasn't load-bearing — the rebuild was.

This is also consistent with the leftover `system-update-debug-run-*` containers we saw during diagnostics (3 stale containers from prior failed attempts) — each pinned to whatever JAR was current at its launch time.

### Next steps (clean-up)

1. **Confirm reproducibility on a clean build.** Revert the `[ZDU-DIAG]` log lines and rerun `./gradlew :datahub-upgrade:bootJar` + the framework's Suite A. If the mutators still load (3 mutators, MigrateAspects initialised, sweep migrates 11 entities), the resolution is confirmed.
2. **Remove the `[ZDU-DIAG]` instrumentation.** A single Edit reverting the diagnostic logs in `SpringStandardPluginConfiguration.java`.
3. **Update Plan 7's live integration check** to no longer skip `upgrade_nonblocking` — the dev-stack constraint is gone.
4. **Add a Gradle task hook** that ensures `bootJar` is re-run before `scripts/dev/datahub-dev.sh test` if the source has changed since the last JAR build, to prevent this stale-JAR class of confusion in the future. (Optional, separate plan.)
