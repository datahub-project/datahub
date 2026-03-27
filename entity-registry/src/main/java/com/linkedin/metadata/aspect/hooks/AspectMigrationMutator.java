package com.linkedin.metadata.aspect.hooks;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class for aspect schema migration mutators.
 *
 * <p>Each concrete subclass handles a single version hop for a single aspect (e.g. v0 → v1).
 * Implementors only need to override {@link #getAspectName()}, {@link #getSourceVersion()}, {@link
 * #getTargetVersion()}, and {@link #transform(RecordTemplate, RetrieverContext)}.
 *
 * <p>Both the read and write paths call {@link #transform} so migration logic stays DRY. The
 * mutator is a no-op when the aspect name does not match or the stored schema version does not
 * equal {@link #getSourceVersion()}.
 *
 * <p>This class is intentionally <em>not</em> a {@link MutationHook}. It defines a single unit of
 * migration work; {@link AspectMigrationMutatorChain} is the {@link MutationHook} that owns
 * orchestration and runs all registered mutators before every other hook.
 */
@Slf4j
public abstract class AspectMigrationMutator {

  /**
   * Baseline schema version. Absent / null {@code schemaVersion} in {@link SystemMetadata} is
   * treated as this version, representing the canonical pre-migration state of all existing
   * aspects.
   */
  public static final long DEFAULT_SCHEMA_VERSION = 1L;

  /** The aspect name this mutator handles (e.g. {@code "ownership"}). */
  @Nonnull
  public abstract String getAspectName();

  /**
   * The schema version of source data this mutator accepts. Absent / null in {@link SystemMetadata}
   * is treated as {@link #DEFAULT_SCHEMA_VERSION} ({@code 1}).
   */
  public abstract long getSourceVersion();

  /**
   * The schema version written to {@link SystemMetadata} after successful migration. Must equal
   * {@code getSourceVersion() + 1} for single-hop migrations.
   */
  public abstract long getTargetVersion();

  /**
   * Apply the schema migration to the aspect payload.
   *
   * <p>The returned {@link RecordTemplate} replaces the original in both read and write paths.
   * Return {@code null} to indicate no transformation was needed (aspect passes through).
   *
   * @param sourceAspect the current aspect payload (already at {@link #getSourceVersion()})
   * @param context retriever context for cross-aspect lookups
   * @return migrated payload, or {@code null} if no change is required
   */
  @Nullable
  protected abstract RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context);

  // ── Read path ──────────────────────────────────────────────────────────────

  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull RetrieverContext retrieverContext) {
    return items.stream()
        .map(
            item -> {
              if (!getAspectName().equals(item.getAspectName())) {
                return Pair.of(item, false);
              }
              RecordTemplate current = item.getRecordTemplate();
              if (current == null) {
                return Pair.of(item, false);
              }
              if (!isSourceVersion(item.getSystemMetadata())) {
                return Pair.of(item, false);
              }
              RecordTemplate migrated = transform(current, retrieverContext);
              if (migrated == null) {
                return Pair.of(item, false);
              }
              // ReadItem has no setRecordTemplate; mutate the underlying DataMap in-place
              // so the change is visible to callers holding a reference to the RecordTemplate.
              current.data().clear();
              current.data().putAll(migrated.data());
              // SystemMetadata on reads is not persisted, but update in-memory so downstream
              // hooks and callers see the correct schema version.
              setTargetVersion(item.getSystemMetadata());
              log.debug(
                  "Read migration applied: aspect={} urn={} v{}→v{}",
                  getAspectName(),
                  item.getUrn(),
                  getSourceVersion(),
                  getTargetVersion());
              return Pair.of(item, true);
            });
  }

  // ── Write path ─────────────────────────────────────────────────────────────

  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream()
        .map(
            item -> {
              if (!getAspectName().equals(item.getAspectName())) {
                return Pair.of(item, false);
              }
              RecordTemplate current = item.getRecordTemplate();
              if (current == null) {
                return Pair.of(item, false);
              }
              if (!isSourceVersion(item.getSystemMetadata())) {
                return Pair.of(item, false);
              }
              RecordTemplate migrated = transform(current, retrieverContext);
              if (migrated == null) {
                return Pair.of(item, false);
              }
              // Mutate the underlying DataMap in-place; ChangeMCP has no setRecordTemplate.
              current.data().clear();
              current.data().putAll(migrated.data());
              setTargetVersion(item.getSystemMetadata());
              log.debug(
                  "Write migration applied: aspect={} urn={} v{}→v{}",
                  getAspectName(),
                  item.getUrn(),
                  getSourceVersion(),
                  getTargetVersion());
              return Pair.of(item, true);
            });
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  /**
   * Returns {@code true} when the stored schema version matches {@link #getSourceVersion()}. A
   * {@code null} system metadata or absent {@code schemaVersion} field is treated as {@link
   * #DEFAULT_SCHEMA_VERSION}.
   */
  private boolean isSourceVersion(@Nullable SystemMetadata systemMetadata) {
    long stored =
        (systemMetadata != null && systemMetadata.hasSchemaVersion())
            ? systemMetadata.getSchemaVersion()
            : DEFAULT_SCHEMA_VERSION;
    return stored == getSourceVersion();
  }

  private void setTargetVersion(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata != null) {
      systemMetadata.setSchemaVersion(getTargetVersion());
    }
  }
}
