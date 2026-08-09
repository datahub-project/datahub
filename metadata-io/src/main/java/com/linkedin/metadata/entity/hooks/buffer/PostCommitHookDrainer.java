package com.linkedin.metadata.entity.hooks.buffer;

import com.linkedin.metadata.buffer.offload.OffloadDrainer;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Background drainer over a {@link PostCommitHookBuffer} of pending post-commit hook replays — now
 * a thin adapter over the framework {@link OffloadDrainer}. All drain infra (cluster-wide
 * single-winner drain lock, bounded {@code PagingPredicate} drain, per-group {@link
 * io.datahubproject.metadata.context.OperationContext} reconstruction, lease guard, retry/DLQ
 * framing) lives in the framework; the hook-specific replay is the {@link HookDrainAction} wired by
 * {@code PostCommitHookBufferFactory}.
 *
 * <p>All pods share one cluster-wide drain lock (Hazelcast), so exactly one pod replays per tick
 * and the rest no-op. The drained batch is grouped by {@link HookContextResolver#groupKey} (e.g. by
 * tenant); each group is replayed under a per-group {@link
 * io.datahubproject.metadata.context.OperationContext} from {@link
 * HookContextResolver#resolveOpContext} so hooks run against the correct (possibly per-tenant)
 * retrievers/catalog.
 *
 * <p><b>Scheduling.</b> {@link #tick()} carries no {@code @Scheduled} annotation; the shared {@code
 * OffloadBufferFactory} registers it with a Spring {@code TaskScheduler} at the use's {@code
 * drainIntervalMs}. This removes the per-use {@code @EnableScheduling} config.
 */
@Slf4j
public class PostCommitHookDrainer {

  private final OffloadDrainer<HookKey, HookPayload> delegate;

  public PostCommitHookDrainer(@Nonnull OffloadDrainer<HookKey, HookPayload> delegate) {
    this.delegate = delegate;
  }

  /** One drain tick. Idempotent under concurrent ticks (cluster-wide drain lock). */
  public void tick() {
    delegate.tick();
  }
}
