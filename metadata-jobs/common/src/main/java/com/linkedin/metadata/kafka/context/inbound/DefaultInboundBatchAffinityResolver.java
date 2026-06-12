package com.linkedin.metadata.kafka.context.inbound;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

/**
 * Default {@link InboundBatchAffinityResolver} — returns a single slice carrying every record under
 * the system context. Used by deployments that do not need to split inbound batches into
 * independently-dispatched units of work.
 *
 * <p>This bean is registered <em>only</em> when no other {@link InboundBatchAffinityResolver} bean
 * exists in the Spring context. Deployments that need affinity-aware splitting register their own
 * {@code @Component} of this type, and this default is not loaded — exactly one bean of {@link
 * InboundBatchAffinityResolver} is ever active.
 */
@Component
@ConditionalOnMissingBean(InboundBatchAffinityResolver.class)
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
