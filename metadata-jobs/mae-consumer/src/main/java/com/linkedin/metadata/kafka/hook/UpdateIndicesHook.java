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
    if (event.getSystemMetadata() != null) {
      if (event.getSystemMetadata().getProperties() != null) {
        if (!Boolean.parseBoolean(event.getSystemMetadata().getProperties().get(FORCE_INDEXING_KEY))
            && UI_SOURCE.equals(event.getSystemMetadata().getProperties().get(APP_SOURCE))
            && !reprocessUIEvents) {
          // If coming from the UI, we pre-process the Update Indices hook as a fast path to avoid
          // Kafka lag
          return;
        }
      }
    }
    updateIndicesService.handleChangeEvent(systemOperationContext, event);
  }
}
