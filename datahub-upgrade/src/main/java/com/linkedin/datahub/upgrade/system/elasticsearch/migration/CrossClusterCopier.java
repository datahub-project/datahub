package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Copies one partition from the source cluster to the target with exact per-document accounting.
 * Implementations differ in where the bytes flow (through this process, or server to server) but
 * share the contract: pausable between units of work and resumable from the checkpoint a pause
 * left, and complete only when every document read was written or already present.
 */
public interface CrossClusterCopier {

  /**
   * Counts cover the partition since it last started over: an attempt resumed from a checkpoint
   * carries what the paused attempt had done.
   *
   * @param total documents the source held for the partition when the copy began (informational)
   * @param read documents the copy processed
   * @param created documents this copy wrote on the target: created, or for a guarded partition
   *     replaced with the source's version because the target's was older
   * @param alreadyPresent documents the target already had (the mirror or an earlier attempt), or
   *     for a guarded partition documents the target held a current version of and left alone
   * @param failures documents the target rejected for another reason
   * @param paused true when the copy stopped early on request and can resume from its checkpoint
   */
  record Result(
      long total, long read, long created, long alreadyPresent, long failures, boolean paused) {

    /** Exact completion: the scan ended and every document read landed or was there already. */
    public boolean isComplete() {
      return !paused && failures == 0 && read == created + alreadyPresent;
    }
  }

  /** Where an attempt stopped: its cursor and the counts so far. */
  record Checkpoint(
      @Nullable String cursor, long read, long created, long alreadyPresent, long failures) {}

  /** Receives the partition's checkpoint between units of work. */
  @FunctionalInterface
  interface Checkpointer {
    void checkpoint(@Nonnull Checkpoint checkpoint);
  }

  /**
   * @param resumeFrom the checkpoint a paused attempt left, or null to start the partition over
   * @param shouldStop polled between units of work; true pauses the copy after a checkpoint
   */
  @Nonnull
  Result copy(
      @Nonnull CopyPartition partition,
      @Nullable Checkpoint resumeFrom,
      @Nonnull Checkpointer checkpointer,
      @Nonnull BooleanSupplier shouldStop)
      throws IOException, InterruptedException;

  /** A short name for logs and the migration row ({@code inProcess}, {@code remoteReindex}). */
  @Nonnull
  String name();
}
