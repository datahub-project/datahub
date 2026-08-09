package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.metadata.buffer.offload.OffloadDrainer;
import com.linkedin.metadata.entity.retention.RetentionKey;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Thin wrapper over the framework {@link OffloadDrainer} for the retention offload. All drain infra
 * — cluster-wide single-winner drain lock, bounded paging drain, grouping by routing key, per-group
 * {@link io.datahubproject.metadata.context.OperationContext} reconstruction, transient-failure
 * backoff, and lease-exceeded guard — lives in {@link OffloadDrainer}; this class exists only to
 * give the drainer a retention-domain bean name and to delegate {@link #tick()}.
 *
 * <p>Construction and scheduling are done by {@code RetentionBufferFactory} via the shared {@code
 * OffloadBufferFactory}; the factory builds the {@link OffloadDrainer} (with a {@link
 * RetentionDrainAction} and a {@link RetentionOffloadResolverAdapter} wrapping the {@link
 * com.linkedin.metadata.entity.retention.RetentionContextResolver}) and registers {@code
 * drainer::tick} with a Spring {@code TaskScheduler} — no {@code @EnableScheduling} config is
 * needed (the old {@code RetentionBufferSchedulingConfig} is deleted).
 */
@Slf4j
public class RetentionDrainer {

  private final OffloadDrainer<RetentionKey, Long> delegate;

  public RetentionDrainer(@Nonnull OffloadDrainer<RetentionKey, Long> delegate) {
    this.delegate = delegate;
  }

  /** One drain tick. Idempotent under concurrent ticks (cluster-wide drain lock). */
  public void tick() {
    delegate.tick();
  }
}
