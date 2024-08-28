package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.common.SystemMetadataServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Component

// TODO check required imports
@Import({
  GraphServiceFactory.class,
  EntitySearchServiceFactory.class,
  TimeseriesAspectServiceFactory.class,
  EntityRegistryFactory.class,
  SystemMetadataServiceFactory.class,
  SearchDocumentTransformerFactory.class
})
public class DataProcessInstanceRunEventHook implements MetadataChangeLogHook {

  private final boolean isEnabled;
  private OperationContext systemOperationContext;
  private final ElasticSearchService elasticSearchService;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public DataProcessInstanceRunEventHook(
      ElasticSearchService elasticSearchService,
      @Nonnull @Value("${myhook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${updateIndices.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.elasticSearchService = elasticSearchService;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public DataProcessInstanceRunEventHook(
      ElasticSearchService elasticSearchService,
      @Nonnull Boolean isEnabled,
      @Nonnull Boolean reprocessUIEvents) {
    this(elasticSearchService, isEnabled, "");
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public DataProcessInstanceRunEventHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (!Objects.equals(event.getAspectName(), DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME)) {
      return;
    }
    Optional<String> docId = SearchUtils.getDocId(event.getEntityUrn());
    if (docId.isEmpty()) {
      return;
    }
    ObjectNode json = JsonNodeFactory.instance.objectNode();
    json.put("hasRunEvents", true);
    elasticSearchService.upsertDocument(
        systemOperationContext, DATA_PROCESS_INSTANCE_ENTITY_NAME, json.toString(), docId.get());
  }
}
