package com.linkedin.metadata.aspect.consistency.scan;

import com.linkedin.metadata.aspect.consistency.SystemMetadataFilter;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.utils.progress.ProgressSnapshot;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Options for a multi-batch entity-check scan over one entity type. */
@Value
@Builder
public class ConsistencyScanRequest {

  @Nonnull String entityType;

  /** Check IDs for this entity type; empty means defaults. */
  @Nullable List<String> checkIds;

  @Nullable SystemMetadataFilter filter;

  @Builder.Default int batchSize = 100;

  /** Delay between batches in milliseconds (0 = no delay). */
  @Builder.Default long delayMs = 0L;

  /**
   * Max entities to scan for this type (0 = unlimited). Absolute scanned count including resume.
   */
  @Builder.Default int limit = 0;

  /** Resume scroll cursor; null to start fresh. */
  @Nullable String scrollId;

  /** Entities already scanned when resuming. */
  @Builder.Default long initialProcessed = 0L;

  /** Issues / fixed / failed already accumulated when resuming. */
  @Builder.Default int initialIssues = 0;

  @Builder.Default int initialFixed = 0;

  @Builder.Default int initialFailed = 0;

  /** Progress INFO throttle interval. */
  @Builder.Default long progressLogIntervalMs = 60_000L;

  /** Warmup before first ETA report. */
  @Builder.Default long progressWarmupMs = 30_000L;

  /**
   * When false, skip entity ETA even if a count is available (e.g. keyAspectOnly=false so SM doc
   * count does not match unique entities).
   */
  @Builder.Default boolean entityEtaEligible = true;

  @Nullable CheckContext checkContext;

  /** Invoked after each non-empty batch for issue handling / fix application. */
  @Nullable BatchHandler onBatch;

  /**
   * Silent checkpoint every batch. Receives scroll id (may be null at end of batch), cumulative
   * counters, and latest progress snapshot.
   */
  @Nullable Consumer<ConsistencyScanCheckpoint> onCheckpoint;

  /** Throttled progress for INFO logging. */
  @Nullable Consumer<ProgressSnapshot> onProgress;

  /** Called once after count (or count failure) before the batch loop. */
  @Nullable Consumer<ConsistencyScanStart> onStart;

  /** Called once after the scan finishes (success path). */
  @Nullable Consumer<ConsistencyScanResult> onComplete;

  /** Return true to stop early (e.g. global limit). */
  @Nullable BooleanSupplier shouldStop;

  /** Optional delay hook for tests; when null, Thread.sleep is used. */
  @Nullable Runnable delayHook;
}
