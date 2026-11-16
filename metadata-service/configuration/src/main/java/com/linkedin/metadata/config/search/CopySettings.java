package com.linkedin.metadata.config.search;

import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** {@code elasticsearch.clusterMigration.<family>.copy}: how and how hard to move the history. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class CopySettings {
  /** False means mirror writes only; system-update neither copies nor verifies. */
  private boolean enabled;

  /** One of {@link CopyMode}'s config values. */
  private String mode;

  /** Concurrent partitions (in-process workers or remote reindex tasks). */
  private int parallelism;

  /** Page size (in-process) or remote {@code size}; sized down per index for large documents. */
  private int batchSize;

  /** Remote reindex throttle; -1 means unthrottled. */
  private int requestsPerSecond;

  /** Time budget per non-blocking run; 0 means none. The copy resumes on the next run. */
  private int maxHours;

  /** Partitions above this many documents are split further by a time range. */
  private long targetDocsPerPartition;

  /** Margin subtracted from the window-pass and delete-log replay start times. */
  private long skewMs;

  /** Keep-alive of the in-process copier's source scroll, renewed per page. */
  private String cursorKeepAlive;

  /** The in-process copier persists its cursor every this many pages. */
  private int checkpointEveryPages;

  /** {@link CopyMode#EXTERNAL} only: when the operator's snapshot of the source was taken. */
  private Long externalWatermarkEpochMs;

  private RemoteCopySettings remote;

  @Nonnull
  public CopyMode copyMode() {
    return mode == null || mode.trim().isEmpty() ? CopyMode.IN_PROCESS : CopyMode.fromConfig(mode);
  }

  @Nonnull
  public RemoteCopySettings getRemote() {
    if (remote == null) {
      remote = RemoteCopySettings.builder().build();
    }
    return remote;
  }
}
