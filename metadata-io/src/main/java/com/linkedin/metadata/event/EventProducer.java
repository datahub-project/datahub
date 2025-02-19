package com.linkedin.metadata.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Interface implemented by producers of {@link com.linkedin.mxe.MetadataAuditEvent}s. */
@Slf4j
public abstract class EventProducer {

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeLog} from a new & previous aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param aspectSpec aspect spec of the aspect being updated
   * @param metadataChangeLog metadata change log to push into MCL kafka topic
   * @return A {@link Future} object that reports when the message has been produced.
   */
  public Future<?> produceMetadataChangeLog(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    metadataChangeLog.setSystemMetadata(
        opContext.withProducerTrace(
            "produceMetadataChangeLog",
            metadataChangeLog.getSystemMetadata(),
            getMetadataChangeLogTopicName(aspectSpec)),
        SetMode.IGNORE_NULL);
    return produceMetadataChangeLog(urn, aspectSpec, metadataChangeLog);
  }

  public abstract Future<?> produceMetadataChangeLog(
      @Nonnull final Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog);

  public abstract String getMetadataChangeLogTopicName(@Nonnull AspectSpec aspectSpec);

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeProposal} as an async update to an entity
   *
   * @param urn the urn associated with the change proposal.
   * @param item Item which includes the metadata change proposal to push into MCP kafka topic.
   * @return A {@link Future} object that reports when the message has been produced.
   */
  public Future<?> produceMetadataChangeProposal(
      @Nonnull OperationContext opContext, @Nonnull final Urn urn, @Nonnull MCPItem item) {
    item.setSystemMetadata(
        opContext.withProducerTrace(
            "produceMetadataChangeProposal",
            item.getSystemMetadata(),
            getMetadataChangeProposalTopicName()));
    return produceMetadataChangeProposal(urn, item.getMetadataChangeProposal());
  }

  @WithSpan
  public abstract Future<?> produceMetadataChangeProposal(
      @Nonnull final Urn urn, @Nonnull MetadataChangeProposal metadataChangeProposal);

  public abstract String getMetadataChangeProposalTopicName();

  public Future<?> produceFailedMetadataChangeProposalAsync(
      @Nonnull OperationContext opContext,
      @Nonnull MCPItem item,
      @Nonnull Set<Throwable> throwables) {
    return produceFailedMetadataChangeProposalAsync(
        opContext, item.getMetadataChangeProposal(), throwables);
  }

  public void produceFailedMetadataChangeProposal(
      @Nonnull OperationContext opContext,
      @Nonnull List<MetadataChangeProposal> mcps,
      @Nonnull Throwable throwable) {
    List<? extends Future<?>> futures =
        mcps.stream()
            .map(
                event ->
                    produceFailedMetadataChangeProposalAsync(opContext, event, Set.of(throwable)))
            .collect(Collectors.toList());

    futures.forEach(
        f -> {
          try {
            f.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @WithSpan
  public abstract Future<?> produceFailedMetadataChangeProposalAsync(
      @Nonnull OperationContext opContext,
      @Nonnull MetadataChangeProposal mcp,
      @Nonnull Set<Throwable> throwables);

  /**
   * Produces a generic platform "event".
   *
   * @param name the name, or type, of the event to produce, as defined in the {@link
   *     EntityRegistry}.
   * @param key an optional partitioning key for the event. If not provided, the name of the event
   *     will be used.
   * @param payload the event payload itself. This will be serialized to JSON and produced as a
   *     system event.
   * @return A {@link Future} object that reports when the message has been produced.
   */
  public abstract Future<?> producePlatformEvent(
      @Nonnull String name, @Nullable String key, @Nonnull PlatformEvent payload);

  public abstract String getPlatformEventTopicName();

  /**
   * Creates an entry on the history log of when the indices were last rebuilt with the latest
   * configuration.
   *
   * @param event the history event to send to the DataHub Upgrade history topic
   */
  public abstract void produceDataHubUpgradeHistoryEvent(@Nonnull DataHubUpgradeHistoryEvent event);
}
