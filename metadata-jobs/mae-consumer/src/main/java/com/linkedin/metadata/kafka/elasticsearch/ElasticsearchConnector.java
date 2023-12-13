package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;

@Slf4j
public class ElasticsearchConnector {

  private final ESBulkProcessor _bulkProcessor;
  private final int _numRetries;

  public ElasticsearchConnector(ESBulkProcessor bulkProcessor, int numRetries) {
    _bulkProcessor = bulkProcessor;
    _numRetries = numRetries;
  }

  /*
    Be careful here, we are mixing `DataHub` change type semantics with `Elasticsearch` concepts.
  */
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
    return new IndexRequest(event.getIndex())
        .id(event.getId())
        .source(event.buildJson())
        .opType(DocWriteRequest.OpType.CREATE);
  }

  @Nonnull
  private static DeleteRequest createDeleteRequest(@Nonnull ElasticEvent event) {
    return new DeleteRequest(event.getIndex()).id(event.getId());
  }

  @Nonnull
  private UpdateRequest createUpsertRequest(@Nonnull ElasticEvent event) {
    return new UpdateRequest(event.getIndex(), event.getId())
        .detectNoop(false)
        .docAsUpsert(true)
        .doc(event.buildJson(), XContentType.JSON)
        .retryOnConflict(_numRetries);
  }
}
