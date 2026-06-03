package com.linkedin.metadata.kafka.usage;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.elasticsearch.JsonElasticEvent;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ElasticsearchDataHubUsageEventIndexer implements DataHubUsageEventIndexer {

  private final ElasticsearchConnector elasticSearchConnector;
  private final String indexName;

  public ElasticsearchDataHubUsageEventIndexer(
      ElasticsearchConnector elasticSearchConnector, IndexConvention indexConvention) {
    this.elasticSearchConnector = elasticSearchConnector;
    this.indexName = indexConvention.getIndexName("datahub_usage_event");
  }

  /**
   * Forward each event in the batch to the {@link ElasticsearchConnector}, which already coalesces
   * them via {@code BulkProcessor}; the indexer itself does not need to manage a separate batch.
   */
  @Override
  public void indexBatch(@Nonnull List<IndexableUsageEvent> events) {
    for (IndexableUsageEvent event : events) {
      JsonElasticEvent elasticEvent = new JsonElasticEvent(event.document().getDocument());
      elasticEvent.setId(event.documentIdWithKafkaOffsetSuffix());
      elasticEvent.setIndex(indexName);
      elasticEvent.setActionType(ChangeType.CREATE);
      elasticSearchConnector.feedElasticEvent(elasticEvent);
    }
  }
}
