package com.linkedin.metadata.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Interface implemented by producers of {@link com.linkedin.mxe.MetadataAuditEvent}s. */
public interface EventProducer {

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeLog} from a new & previous aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param aspectSpec aspect spec of the aspect being updated
   * @param metadataChangeLog metadata change log to push into MCL kafka topic
   * @return A {@link Future} object that reports when the message has been produced.
   */
  Future<?> produceMetadataChangeLog(
      @Nonnull final Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog);

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeProposal} as an async update to an entity
   *
   * @param urn the urn associated with the change proposal.
   * @param metadataChangeProposal metadata change proposal to push into MCP kafka topic.
   * @return A {@link Future} object that reports when the message has been produced.
   */
  @WithSpan
  Future<?> produceMetadataChangeProposal(
      @Nonnull final Urn urn, @Nonnull MetadataChangeProposal metadataChangeProposal);

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
  Future<?> producePlatformEvent(
      @Nonnull String name, @Nullable String key, @Nonnull PlatformEvent payload);

  /**
   * Creates an entry on the history log of when the indices were last rebuilt with the latest
   * configuration.
   *
   * @param event the history event to send to the DataHub Upgrade history topic
   */
  void produceDataHubUpgradeHistoryEvent(@Nonnull DataHubUpgradeHistoryEvent event);
}
