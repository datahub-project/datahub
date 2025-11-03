package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.gms.factory.common.SystemMetadataServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

// TODO: Backfill tests for this class in UpdateIndicesHookTest.java
@Slf4j
@Component
@Import({
  EntitySearchServiceFactory.class,
  TimeseriesAspectServiceFactory.class,
  EntityRegistryFactory.class,
  SystemMetadataServiceFactory.class,
  SearchDocumentTransformerFactory.class
})
public class UpdateIndicesHook implements MetadataChangeLogHook {

  protected final UpdateIndicesService updateIndicesService;
  private final boolean isEnabled;
  private final boolean reprocessUIEvents;
  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public UpdateIndicesHook(
      UpdateIndicesService updateIndicesService,
      @Nonnull @Value("${updateIndices.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${featureFlags.preProcessHooks.reprocessEnabled:false}")
          Boolean reprocessUIEvents,
      @Nonnull @Value("${updateIndices.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.updateIndicesService = updateIndicesService;
    this.isEnabled = isEnabled;
    this.reprocessUIEvents = reprocessUIEvents;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public UpdateIndicesHook(
      UpdateIndicesService updateIndicesService,
      @Nonnull Boolean isEnabled,
      @Nonnull Boolean reprocessUIEvents) {
    this(updateIndicesService, isEnabled, reprocessUIEvents, "");
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public UpdateIndicesHook init(@javax.annotation.Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    invokeBatch(Collections.singletonList(event));
  }

  @Override
  public void invokeBatch(@Nonnull final Collection<MetadataChangeLog> events) {
    // Filter events that should be processed
    List<MetadataChangeLog> eventsToProcess =
        events.stream().filter(this::shouldProcessEvent).collect(Collectors.toList());

    if (!eventsToProcess.isEmpty()) {
      log.info(
          "Processing batch of {} MCL events with UpdateIndicesService", eventsToProcess.size());
      updateIndicesService.handleChangeEvents(systemOperationContext, eventsToProcess);
    } else {
      log.debug("No MCL events to process in batch of {} events", events.size());
    }
  }

  /** Determines if an event should be processed based on UI source and reprocessing flags */
  private boolean shouldProcessEvent(MetadataChangeLog event) {
    if (event.getSystemMetadata() != null) {
      if (event.getSystemMetadata().getProperties() != null) {
        if (!Boolean.parseBoolean(event.getSystemMetadata().getProperties().get(FORCE_INDEXING_KEY))
            && UI_SOURCE.equals(event.getSystemMetadata().getProperties().get(APP_SOURCE))
            && !reprocessUIEvents) {
          // If coming from the UI, we pre-process the Update Indices hook as a fast path to avoid
          // Kafka lag
          return false;
        }
      }
    }
    return true;
  }
}
