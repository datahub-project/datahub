package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.common.SystemMetadataServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.MetadataChangeLog;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

// TODO: Backfill tests for this class in UpdateIndicesHookTest.java
@Slf4j
@Component
@Import({
  GraphServiceFactory.class,
  EntitySearchServiceFactory.class,
  TimeseriesAspectServiceFactory.class,
  EntityRegistryFactory.class,
  SystemMetadataServiceFactory.class,
  SearchDocumentTransformerFactory.class
})
public class UpdateIndicesHook implements MetadataChangeLogHook {

  protected final UpdateIndicesService _updateIndicesService;
  private final boolean _isEnabled;

  public UpdateIndicesHook(
      UpdateIndicesService updateIndicesService,
      @Nonnull @Value("${updateIndices.enabled:true}") Boolean isEnabled) {
    _updateIndicesService = updateIndicesService;
    _isEnabled = isEnabled;
  }

  @Override
  public boolean isEnabled() {
    return _isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (event.getSystemMetadata() != null) {
      if (event.getSystemMetadata().getProperties() != null) {
        if (UI_SOURCE.equals(event.getSystemMetadata().getProperties().get(APP_SOURCE))) {
          // If coming from the UI, we pre-process the Update Indices hook as a fast path to avoid
          // Kafka lag
          return;
        }
      }
    }
    _updateIndicesService.handleChangeEvent(event);
  }
}
