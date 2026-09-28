package com.linkedin.metadata.kafka.context.inbound;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Partitions an inbound batch of consumer records into one or more affinity slices, each carrying
 * the {@link OperationContext} that downstream processing should run under for that slice's
 * records.
 *
 * <p>This is a <em>single-bean</em> extension point — exactly one implementation is ever active in
 * the Spring context. The default ({@link DefaultInboundBatchAffinityResolver}) is registered by
 * {@link InboundContextResolverFactory} via {@code @Bean @ConditionalOnMissingBean}, so it loads
 * only when no other implementation is present; deployments that need affinity-aware splitting
 * register their own {@code InboundBatchAffinityResolver} bean and the default is shadowed entirely
 * (not loaded). No {@code @Primary} required.
 *
 * <p>The resolver operates on raw {@link ConsumerRecord}s (not decoded payloads) so it can read
 * record headers / keys for grouping without forcing a decode step. Decoding happens inside each
 * slice in the calling listener.
 */
public interface InboundBatchAffinityResolver {

  /**
   * A group of consumer records that share an operational affinity and are dispatched together
   * under a single {@link OperationContext}.
   */
  record Slice<R>(
      @Nonnull OperationContext context, @Nonnull List<ConsumerRecord<String, R>> records) {}

  /**
   * Partitions {@code records} into one or more {@link Slice}s. Implementations must preserve
   * insertion order of records within each slice. May return an empty list when {@code records} is
   * empty.
   *
   * @param records the inbound batch as received from the Kafka container
   * @param consumerGroupId the consumer group reading this batch (useful for resolver-internal
   *     metric attribution)
   * @param systemContext the system-level {@link OperationContext} that the calling listener was
   *     initialised with; used as the slice context in the default OSS impl
   * @return one or more slices; never null
   */
  @Nonnull
  <R> List<Slice<R>> partition(
      @Nonnull List<ConsumerRecord<String, R>> records,
      @Nonnull String consumerGroupId,
      @Nonnull OperationContext systemContext);
}
