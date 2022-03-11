package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.events.metadata.ChangeType;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;


@Slf4j
public class ElasticsearchConnector {

  private BulkProcessor _bulkProcessor;
  private static final int DEFAULT_NUMBER_OF_RETRIES = 3; // TODO: Test and also add these into config
  private static final long DEFAULT_RETRY_INTERVAL = 1L;

  public ElasticsearchConnector(RestHighLevelClient elasticSearchRestClient, Integer bulkRequestsLimit,
      Integer bulkFlushPeriod) {
    initBulkProcessor(elasticSearchRestClient, bulkRequestsLimit, bulkFlushPeriod);
  }

  private void initBulkProcessor(RestHighLevelClient elasticSearchRestClient, Integer bulkRequestsLimit,
      Integer bulkFlushPeriod) {
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {

      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        log.info("Successfully feeded bulk request. Number of events: " + response.getItems().length + " Took time ms: "
            + response.getIngestTookInMillis());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        log.error("Error feeding bulk request. No retries left", failure);
      }
    };

    _bulkProcessor = BulkProcessor.builder(
        (request, bulkListener) -> elasticSearchRestClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
        listener)
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(DEFAULT_RETRY_INTERVAL),
            DEFAULT_NUMBER_OF_RETRIES))
        .build();
  }

  public void feedElasticEvent(@Nonnull ElasticEvent event) {
    if (event.getActionType().equals(ChangeType.DELETE)) {
      _bulkProcessor.add(createDeleteRequest(event));
    } else if (event.getActionType().equals(ChangeType.CREATE)) {
      _bulkProcessor.add(createIndexRequest(event));
    } else if (event.getActionType().equals(ChangeType.UPDATE)) {
      _bulkProcessor.add(createUpsertRequest(event));
    }
  }

  @Nonnull
  private static IndexRequest createIndexRequest(@Nonnull ElasticEvent event) {
    return new IndexRequest(event.getIndex()).id(event.getId())
        .source(event.buildJson())
        .opType(DocWriteRequest.OpType.CREATE);
  }

  @Nonnull
  private static DeleteRequest createDeleteRequest(@Nonnull ElasticEvent event) {
    return new DeleteRequest(event.getIndex()).id(event.getId());
  }

  @Nonnull
  private static UpdateRequest createUpsertRequest(@Nonnull ElasticEvent event) {
    final IndexRequest indexRequest = new IndexRequest(event.getIndex()).id(event.getId()).source(event.buildJson());
    return new UpdateRequest(event.getIndex(), event.getId()).doc(event.buildJson())
        .detectNoop(false)
        .upsert(indexRequest);
  }
}

