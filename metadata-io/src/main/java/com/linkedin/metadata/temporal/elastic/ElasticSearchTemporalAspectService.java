package com.linkedin.metadata.temporal.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.temporal.TemporalAspectService;
import com.linkedin.metadata.temporal.elastic.indexbuilder.TemporalAspectIndexBuilders;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;


@Slf4j
public class ElasticSearchTemporalAspectService implements TemporalAspectService {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final IndexConvention _indexConvention;
  private final BulkProcessor _bulkProcessor;
  private final TemporalAspectIndexBuilders _indexBuilders;
  private final RestHighLevelClient _searchClient;

  public ElasticSearchTemporalAspectService(@Nonnull RestHighLevelClient searchClient,
      @Nonnull IndexConvention indexConvention, @Nonnull TemporalAspectIndexBuilders indexBuilders,
      int bulkRequestsLimit, int bulkFlushPeriod, int numRetries, long retryInterval) {
    _indexConvention = indexConvention;
    _indexBuilders = indexBuilders;
    _searchClient = searchClient;
    _bulkProcessor = BulkProcessor.builder(
        (request, bulkListener) -> searchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
        BulkListener.getInstance())
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
        .build();
  }

  @Override
  public void configure() {
    _indexBuilders.buildAll();
  }

  @Override
  public void upsertDocument(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull String document) {
    String indexName = _indexConvention.getTemporalAspectIndexName(entityName, aspectName);
    String docId;
    try {
      docId = toDocId(document);
    } catch (JsonProcessingException e) {
      log.error("Failed to get document ID for document: {}", document);
      return;
    }
    final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
    final UpdateRequest updateRequest =
        new UpdateRequest(indexName, docId).doc(document, XContentType.JSON).detectNoop(false).upsert(indexRequest);
    _bulkProcessor.add(updateRequest);
  }

  private String toDocId(@Nonnull final String document) throws JsonProcessingException {
    return DigestUtils.md5Hex(String.valueOf(OBJECT_MAPPER.readTree(document).hashCode()));
  }
}
