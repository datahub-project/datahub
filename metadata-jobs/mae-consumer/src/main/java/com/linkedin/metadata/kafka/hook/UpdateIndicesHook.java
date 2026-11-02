package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.gms.factory.common.SystemMetadataServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
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

  private static final int LARGE_BATCH_THRESHOLD = 500;

  protected final UpdateIndicesService updateIndicesService;
  private final boolean isEnabled;
  private final boolean reprocessUIEvents;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public UpdateIndicesHook(
      UpdateIndicesService updateIndicesService,
      @Nonnull @Value("${updateIndices.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${featureFlags.preProcessHooks.uiEnabled:true}") Boolean uiPreprocessEnabled,
      @Nonnull @Value("${featureFlags.preProcessHooks.reprocessEnabled:false}")
          Boolean reprocessUIEvents,
      @Nonnull @Value("${updateIndices.consumerGroupSuffix}") String consumerGroupSuffix,
      @Value("${MAE_CONSUMER_ENABLED:false}") String maeConsumerEnabled,
      @Value("${MCL_CONSUMER_ENABLED:false}") String mclConsumerEnabled) {
    this.updateIndicesService = updateIndicesService;
    this.isEnabled = isEnabled;
    this.reprocessUIEvents = reprocessUIEvents;
    this.consumerGroupSuffix = consumerGroupSuffix;
    // GMS always constructs this bean (embedded MAE lives on the GMS classpath). Gate on MCL
    // consumption so kubernetesScaleDown can set PRE_PROCESS_HOOKS_UI_ENABLED=false while
    // MAE_CONSUMER_ENABLED=false. Standalone MAE sets MAE_CONSUMER_ENABLED=true. Helm sets the
    // same PRE_PROCESS_HOOKS_* pair on GMS and MAE; default MAE is uiEnabled=true (skip UI
    // events because GMS already indexed them), not the old both-false template.
    if (Boolean.TRUE.equals(isEnabled)) {
      PreProcessHooks hooks = new PreProcessHooks();
      hooks.setUiEnabled(Boolean.TRUE.equals(uiPreprocessEnabled));
      hooks.setReprocessEnabled(Boolean.TRUE.equals(reprocessUIEvents));
      PreProcessHooks.validateWhenConsumingMcl(
          hooks, PreProcessHooks.isMclConsumerEnabled(maeConsumerEnabled, mclConsumerEnabled));
    }
  }

  @VisibleForTesting
  public UpdateIndicesHook(
      UpdateIndicesService updateIndicesService,
      @Nonnull Boolean isEnabled,
      @Nonnull Boolean uiPreprocessEnabled,
      @Nonnull Boolean reprocessUIEvents,
      @Nonnull String consumerGroupSuffix,
      boolean mclConsumerEnabled) {
    this(
        updateIndicesService,
        isEnabled,
        uiPreprocessEnabled,
        reprocessUIEvents,
        consumerGroupSuffix,
        mclConsumerEnabled ? "true" : "false",
        "false");
  }

  @VisibleForTesting
  public UpdateIndicesHook(
      UpdateIndicesService updateIndicesService,
      @Nonnull Boolean isEnabled,
      @Nonnull Boolean reprocessUIEvents,
      @Nonnull String consumerGroupSuffix) {
    this(updateIndicesService, isEnabled, true, reprocessUIEvents, consumerGroupSuffix, true);
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
  public void invoke(
      @Nonnull OperationContext operationContext, @Nonnull final MetadataChangeLog event) {
    if (shouldProcessEvent(event)) {
      updateIndicesService.handleChangeEvent(operationContext, event);
      updateIndicesService.flushAndWaitIfConfigured();
    }
  }

  @Override
  public void invokeBatch(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull final Collection<MetadataChangeLog> events) {
    // Filter events to only process those that should be processed
    List<MetadataChangeLog> eventsToProcess =
        events.stream().filter(this::shouldProcessEvent).collect(Collectors.toList());

    if (!eventsToProcess.isEmpty()) {
      if (eventsToProcess.size() >= LARGE_BATCH_THRESHOLD) {
        log.info(
            "Processing large batch of {} MCL events with UpdateIndicesService",
            eventsToProcess.size());
      } else {
        log.debug(
            "Processing batch of {} MCL events with UpdateIndicesService", eventsToProcess.size());
      }
      updateIndicesService.handleChangeEvents(systemOperationContext, eventsToProcess);
      updateIndicesService.flushAndWaitIfConfigured();
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
