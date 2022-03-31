package com.linkedin.metadata.kafka.hook.ingestion;

import com.datahub.metadata.ingestion.IngestionScheduler;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.ingestion.IngestionSchedulerFactory;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;


/**
 * This hook updates a stateful {@link IngestionScheduler} of Ingestion Runs for Ingestion Sources defined
 * within DataHub.
 */
@Slf4j
@Component
@Singleton
@Import({EntityRegistryFactory.class, IngestionSchedulerFactory.class})
public class IngestionSchedulerHook implements MetadataChangeLogHook {

  private final EntityRegistry _entityRegistry;
  private final IngestionScheduler _scheduler;

  @Autowired
  public IngestionSchedulerHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final IngestionScheduler scheduler
  ) {
    _entityRegistry = entityRegistry;
    _scheduler = scheduler;
  }

  @Override
  public void init() {
    _scheduler.init();
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    if (isEligibleForProcessing(event)) {

      log.info("Received {} to Ingestion Source. Rescheduling the source (if applicable). urn: {}, key: {}.",
          event.getChangeType(),
          event.getEntityUrn(),
          event.getEntityKeyAspect());

      final Urn urn = getUrnFromEvent(event);

      if (ChangeType.DELETE.equals(event.getChangeType())) {
        _scheduler.unscheduleNextIngestionSourceExecution(urn);
      } else {
        // Update the scheduler to reflect the latest changes.
        final DataHubIngestionSourceInfo info = getInfoFromEvent(event);
        _scheduler.scheduleNextIngestionSourceExecution(urn, info);
      }
    }
  }

  /**
   * Returns true if the event should be processed, which is only true if the event represents a create, update, or delete
   * of an Ingestion Source Info aspect, which in turn contains the schedule associated with the source.
   */
  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    return Constants.INGESTION_INFO_ASPECT_NAME.equals(event.getAspectName())
        && (ChangeType.DELETE.equals(event.getChangeType())
        || ChangeType.UPSERT.equals(event.getChangeType())
        || ChangeType.CREATE.equals(event.getChangeType()));
  }

  /**
   * Extracts and returns an {@link Urn} from a {@link MetadataChangeLog}. Extracts from either an entityUrn
   * or entityKey field, depending on which is present.
   */
  private Urn getUrnFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException("Failed to get urn from MetadataChangeLog event. Skipping processing.", e);
    }
    // Extract an URN from the Log Event.
    return EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());
  }

  /**
   * Deserializes and returns an instance of {@link DataHubIngestionSourceInfo} extracted from a {@link MetadataChangeLog} event.
   * The incoming event is expected to have a populated "aspect" field.
   */
  private DataHubIngestionSourceInfo getInfoFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException("Failed to get Ingestion Source info from MetadataChangeLog event. Skipping processing.", e);
    }
    return (DataHubIngestionSourceInfo) GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        entitySpec.getAspectSpec(Constants.INGESTION_INFO_ASPECT_NAME));
  }

  @VisibleForTesting
  IngestionScheduler scheduler() {
    return _scheduler;
  }
}
