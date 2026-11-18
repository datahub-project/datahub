package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.config.search.SearchComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Keys of the flat {@code dataHubUpgradeResult.result} map that records one non-entity index
 * family's move from a serving cluster to a target cluster.
 *
 * <p>Unlike {@link IncrementalReindexState}, a row is keyed by family, source and target rather
 * than by upgrade version, so a copy that spans deploys resumes instead of being re-planned.
 */
public final class ClusterMigrationState {
  public static final String UPGRADE_ID_PREFIX = "ClusterMigration";

  public static final String STATUS = "status";
  public static final String FAMILY = "family";
  public static final String SOURCE_CLUSTER = "sourceCluster";
  public static final String TARGET_CLUSTER = "targetCluster";
  public static final String REVISION_STARTED = "revisionStarted";

  /**
   * The system update (version and revision) that last prepared the row. A deploy is a rollout: a
   * prepare under a new one records {@link #ROLLOUT_STARTED_AT}, a prepare under the same one (a
   * re-run of the job) leaves it alone, since the pods have not changed.
   */
  public static final String PREPARED_BY = "preparedBy";

  /**
   * When the system update named by {@link #PREPARED_BY} prepared the row, before its pods rolled.
   * A mirror marker is the start time of the pod that recorded it, so a marker older than this was
   * recorded by a pod of an earlier rollout and does not count; the pods of this rollout, which
   * start after the blocking job ends, record theirs over it. Nothing ever deletes a marker.
   */
  public static final String ROLLOUT_STARTED_AT = "rolloutStartedAt";

  /**
   * Whether the non-blocking step copies the family's history, as the prepare step decided: the
   * configuration's copy setting, unless the reverse migration has verified, after which nothing is
   * left to copy and the pair mirrors only whatever the setting says.
   */
  public static final String COPY_ENABLED = "copyEnabled";

  /** Recorded once by the blocking prepare step; a lower bound on every window computed from it. */
  public static final String MIRROR_START_TIME = "mirrorStartTime";

  /** {@code mirrorActiveSince.<process>}: when that process first mirrored a write (epoch ms). */
  public static final String MIRROR_ACTIVE_SINCE_PREFIX = "mirrorActiveSince.";

  /**
   * The row every live writer (GMS, the MAE consumer) reports to once a minute while a second
   * cluster is declared, whether or not it migrates anything: {@code writer.<instance>} holds the
   * process's start time and the time of its last report. The copy waits until no writer that
   * started before the rollout's stamp is still reporting, which is how it knows that the previous
   * rollout's pods, which do not mirror, have stopped writing. Those pods report although their
   * configuration knows no migration, which is why the row is shared rather than per family.
   */
  public static final String WRITERS_UPGRADE_ID = UPGRADE_ID_PREFIX + "Writers";

  public static final String WRITER_PREFIX = "writer.";

  /** A writer that has not reported for this long is gone: three missed minute reports. */
  public static final long WRITER_STALE_AFTER_MS = 3 * 60_000L;

  /** A report older than this is dropped from the row the next time a writer reports. */
  public static final long WRITER_RETENTION_MS = 24 * 60 * 60_000L;

  public static final String COPY_MODE = "copyMode";
  public static final String COPY_START_TIME = "copyStartTime";
  public static final String COPY_COMPLETE_TIME = "copyCompleteTime";
  public static final String PARTITION_COUNT = "partitionCount";

  /** {@code partition.<n>.<property>}: one partition's spec, status, cursor and counts. */
  public static final String PARTITION_PREFIX = "partition.";

  public static final String WINDOW_PREFIX = "window.";
  public static final String REPLAY_WATERMARK = "replay.watermark";
  public static final String VERIFY_PREFIX = "verify.";

  /**
   * Usage events mirror into a concrete live staging index on the target rather than its rollover
   * alias or data stream. These keys record that index, the serving cluster's write index when the
   * mirror started (the copy plan, or the prepare of a mirror-only migration) and whether the sweep
   * that finishes the staging index once the family serves from the target is {@code pending} or
   * {@code done}.
   */
  public static final String USAGE_LIVE_STAGING_INDEX = "usage.liveStagingIndex";

  public static final String USAGE_LAST_SOURCE_WRITE_INDEX = "usage.lastSourceWriteIndex";
  public static final String USAGE_POST_FLIP_SWEEP = "usage.postFlipSweep";

  /** Processes whose mirror must be live before the copy may read the source. */
  public enum Process {
    GMS("gms"),
    MAE("mae"),
    UPGRADE("upgrade");

    private final String key;

    Process(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
    }
  }

  public enum Status {
    PREPARED,
    COPYING,
    WINDOW,
    REPLAY,
    ADOPTING,
    VERIFIED,
    FAILED;

    public boolean isTerminal() {
      return this == VERIFIED;
    }

    /**
     * Whether the non-blocking step is part way through moving data: partitions open, the window
     * pass, the delete replay or a staging adoption. A prepared migration has only created its
     * target indices and started the mirror, so the family can be pointed elsewhere and back
     * without losing anything; the reverse mirror after a flip stays prepared for exactly that.
     */
    public boolean hasCopyInFlight() {
      return this == COPYING || this == WINDOW || this == REPLAY || this == ADOPTING;
    }
  }

  /** One live writer's report: the process's start time and when it last reported. */
  public record Writer(@Nonnull String instance, long startedAt, long lastSeen) {}

  private ClusterMigrationState() {}

  @Nonnull
  public static String writerKey(@Nonnull final String instance) {
    return WRITER_PREFIX + instance;
  }

  @Nonnull
  public static String writerValue(final long startedAt, final long lastSeen) {
    return startedAt + ":" + lastSeen;
  }

  /**
   * The report under a {@code writer.<instance>} key, or null for a key or value not of that shape.
   */
  @Nullable
  public static Writer parseWriter(@Nonnull final String key, @Nullable final String value) {
    if (!key.startsWith(WRITER_PREFIX) || value == null) {
      return null;
    }
    final int colon = value.indexOf(':');
    if (colon <= 0) {
      return null;
    }
    try {
      return new Writer(
          key.substring(WRITER_PREFIX.length()),
          Long.parseLong(value.substring(0, colon)),
          Long.parseLong(value.substring(colon + 1)));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Nonnull
  public static List<Writer> writers(@Nonnull final Map<String, String> writersRow) {
    final List<Writer> writers = new ArrayList<>();
    for (Map.Entry<String, String> entry : writersRow.entrySet()) {
      final Writer writer = parseWriter(entry.getKey(), entry.getValue());
      if (writer != null) {
        writers.add(writer);
      }
    }
    return writers;
  }

  /**
   * The writers that started before {@code rolloutStartedAt} and reported within {@link
   * #WRITER_STALE_AFTER_MS} of {@code now}: pods of the previous rollout still serving, which do
   * not mirror the family being migrated.
   */
  @Nonnull
  public static List<Writer> previousRolloutWritersReporting(
      @Nonnull final Map<String, String> writersRow, final long rolloutStartedAt, final long now) {
    return writers(writersRow).stream()
        .filter(
            writer ->
                writer.startedAt() < rolloutStartedAt
                    && writer.lastSeen() >= now - WRITER_STALE_AFTER_MS)
        .toList();
  }

  /** {@code ClusterMigration_<family>_<source>_to_<target>}, the upgrade id and row identity. */
  @Nonnull
  public static String upgradeId(
      @Nonnull final SearchComponent family,
      @Nonnull final String sourceCluster,
      @Nonnull final String targetCluster) {
    return UPGRADE_ID_PREFIX
        + "_"
        + family.getConfigKey().toLowerCase(Locale.ROOT)
        + "_"
        + sourceCluster
        + "_to_"
        + targetCluster;
  }

  @Nonnull
  public static String mirrorActiveSinceKey(@Nonnull final Process process) {
    return MIRROR_ACTIVE_SINCE_PREFIX + process.getKey();
  }

  @Nonnull
  public static String partitionKey(final int partition, @Nonnull final String property) {
    return PARTITION_PREFIX + partition + "." + property;
  }

  @Nonnull
  public static Optional<Status> getStatus(@Nonnull final Map<String, String> result) {
    final String raw = result.get(STATUS);
    if (raw == null || raw.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(Status.valueOf(raw));
  }

  @Nonnull
  public static Optional<Long> getLong(
      @Nonnull final Map<String, String> result, @Nonnull final String key) {
    final String raw = result.get(key);
    if (raw == null || raw.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(Long.parseLong(raw));
  }

  @Nonnull
  public static Optional<Long> mirrorActiveSince(
      @Nonnull final Map<String, String> result, @Nonnull final Process process) {
    return getLong(result, mirrorActiveSinceKey(process));
  }

  /**
   * Whether the marker under {@code markerKey} was recorded by a pod of the current rollout: it
   * exists and is no older than {@link #ROLLOUT_STARTED_AT}, or any marker while the row carries no
   * stamp.
   */
  public static boolean marksCurrentRollout(
      @Nonnull final Map<String, String> result, @Nonnull final String markerKey) {
    final long rolloutStartedAt = getLong(result, ROLLOUT_STARTED_AT).orElse(Long.MIN_VALUE);
    return getLong(result, markerKey).map(marker -> marker >= rolloutStartedAt).orElse(false);
  }

  /**
   * The moment from which every live writer of the current rollout is known to mirror: the later of
   * the GMS and MAE markers, or empty while either is missing or was recorded by a pod of an
   * earlier rollout. Reads from the source before this instant could miss a write that only the
   * old, unmirrored pods made.
   */
  @Nonnull
  public static Optional<Long> liveWritersMirroringSince(
      @Nonnull final Map<String, String> result) {
    for (Process process : List.of(Process.GMS, Process.MAE)) {
      if (!marksCurrentRollout(result, mirrorActiveSinceKey(process))) {
        return Optional.empty();
      }
    }
    return Optional.of(
        Math.max(
            mirrorActiveSince(result, Process.GMS).orElseThrow(),
            mirrorActiveSince(result, Process.MAE).orElseThrow()));
  }

  @Nullable
  public static String get(@Nonnull final Map<String, String> result, @Nonnull final String key) {
    return result.get(key);
  }
}
