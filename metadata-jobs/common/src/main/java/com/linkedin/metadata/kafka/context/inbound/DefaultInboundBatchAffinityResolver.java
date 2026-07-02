package com.linkedin.metadata.kafka.context.inbound;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Default {@link InboundBatchAffinityResolver} — returns a single slice carrying every record under
 * the system context. Backward-compatible with pre-PR4 behavior: callers that don't override this
 * resolver see exactly the same dispatch shape they had before.
 *
 * <p>Registered conditionally by {@link InboundContextResolverFactory} so deployments that ship
 * their own {@link InboundBatchAffinityResolver} bean shadow this default cleanly — exactly one
 * resolver bean is ever active in the Spring context. No {@code @Primary} required.
 */
public class DefaultInboundBatchAffinityResolver implements InboundBatchAffinityResolver {

  @Override
  @Nonnull
  public <R> List<Slice<R>> partition(
      @Nonnull List<ConsumerRecord<String, R>> records,
      @Nonnull String consumerGroupId,
      @Nonnull OperationContext systemContext) {
    if (records.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(new Slice<>(systemContext, records));
  }
}
