package com.linkedin.metadata.config.hooks;

import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.OffloadBufferProperties;
import com.linkedin.metadata.config.offload.SizingPolicy;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * POJO representing the "datahub.postCommitHook.buffer" tuning block in application.yaml, as a
 * hook-specific specialization of the framework {@link OffloadBufferProperties}. On/off is {@code
 * featureFlags.postCommitHookBufferEnabled}, not this POJO. The buffer is backed by the shared
 * embedded Hazelcast instance; without it, hooks run synchronously on the ingest thread (legacy
 * behavior).
 *
 * <p>Hook semantics are fixed here: {@link SizingPolicy#REJECT_AT_CAP} (no-loss bound — at cap,
 * {@code enqueue} returns {@code false} and the caller runs the hook synchronously) and {@link
 * MergePolicy#NO_COALESCE} (every committed MCL is a distinct fact, replayed exactly once). The
 * no-arg constructor sets these and the hook map names as defaults; Spring overrides any that
 * appear in {@code datahub.postCommitHook.buffer.*}.
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class PostCommitHookBufferProperties extends OffloadBufferProperties {

  /** Hook-specific defaults. Spring binds {@code datahub.postCommitHook.buffer.*} over these. */
  public PostCommitHookBufferProperties() {
    setMapName("post-commit-hook-pending");
    setLockMapName("post-commit-hook-drain-lock");
    // Preserve the historical seq-map name (mapName + ".seq") so in-flight Hazelcast entries
    // survive the framework migration unchanged.
    setSeqMapName("post-commit-hook-pending.seq");
    setMaxPendingEntries(100_000);
    setDrainBatchSize(500);
    setDrainIntervalMs(2000);
    setDrainLockLeaseMs(60_000);
    setSizingPolicy(SizingPolicy.REJECT_AT_CAP);
    setMergePolicy(MergePolicy.NO_COALESCE);
  }
}
