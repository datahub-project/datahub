package com.linkedin.metadata.graph.elastic;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.buildQuery;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;


@Slf4j
@RequiredArgsConstructor
public class ESGraphWriteDAO {
  private final RestHighLevelClient client;
  private final IndexConvention indexConvention;
  private final BulkProcessor bulkProcessor;

  /**
   * Updates or inserts the given search document.
   *
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String docId, @Nonnull String document) {
    final IndexRequest indexRequest =
        new IndexRequest(indexConvention.getIndexName(INDEX_NAME)).id(docId).source(document, XContentType.JSON);
    final UpdateRequest updateRequest =
        new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId).doc(document, XContentType.JSON)
            .detectNoop(false)
            .upsert(indexRequest);
    bulkProcessor.add(updateRequest);
  }

  public BulkByScrollResponse deleteByQuery(@Nullable final String sourceType, @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType, @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes, @Nonnull final RelationshipFilter relationshipFilter) {
    BoolQueryBuilder finalQuery =
        buildQuery(sourceType == null ? ImmutableList.of() : ImmutableList.of(sourceType), sourceEntityFilter,
            destinationType == null ? ImmutableList.of() : ImmutableList.of(destinationType), destinationEntityFilter,
            relationshipTypes, relationshipFilter);

    DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();

    deleteByQueryRequest.setQuery(finalQuery);

    deleteByQueryRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final BulkByScrollResponse deleteResponse = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
      return deleteResponse;
    } catch (IOException e) {
      log.error("ERROR: Failed to delete by query. See stacktrace for a more detailed error:");
      e.printStackTrace();
    }
    return null;
  }
}
