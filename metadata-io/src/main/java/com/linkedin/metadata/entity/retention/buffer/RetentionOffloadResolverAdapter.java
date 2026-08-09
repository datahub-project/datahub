package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.metadata.buffer.offload.OffloadContextResolver;
import com.linkedin.metadata.buffer.offload.UnresolvableOffloadKeyException;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.UnresolvableRetentionKeyException;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Bridges the services {@link RetentionContextResolver} (which lives in {@code
 * metadata-service:services} and so cannot itself extend the framework {@link
 * OffloadContextResolver} from {@code metadata-io}) to the framework {@link OffloadContextResolver}
 * drain-side seam. The framework {@code OffloadDrainer} drives this adapter without knowing the
 * services resolver; the enqueue-side {@code enrichKey} stays on the services resolver (used by
 * {@link CoalesceRetentionBuffer}).
 *
 * <p>Permanent {@link UnresolvableRetentionKeyException}s are translated to {@link
 * UnresolvableOffloadKeyException} so the framework drainer's drop-vs-backoff contract applies.
 */
public class RetentionOffloadResolverAdapter implements OffloadContextResolver<RetentionKey> {

  private final RetentionContextResolver delegate;

  public RetentionOffloadResolverAdapter(@Nonnull RetentionContextResolver delegate) {
    this.delegate = delegate;
  }

  @Override
  @Nonnull
  public String groupKey(@Nonnull RetentionKey key) {
    try {
      return delegate.groupKey(key);
    } catch (UnresolvableRetentionKeyException e) {
      throw new UnresolvableOffloadKeyException(e.getMessage(), e);
    }
  }

  @Override
  @Nonnull
  public OperationContext resolveOpContext(
      @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext) {
    try {
      return delegate.resolveOpContext(key, systemOperationContext);
    } catch (UnresolvableRetentionKeyException e) {
      throw new UnresolvableOffloadKeyException(e.getMessage(), e);
    }
  }
}
