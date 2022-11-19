package com.linkedin.metadata.graph.elastic;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.buildQuery;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;


@Slf4j
@RequiredArgsConstructor
public class ESGraphWriteDAO {
  private final IndexConvention indexConvention;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;

  private static final String ES_WRITES_METRIC = "num_elasticSearch_writes";

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
                .retryOnConflict(numRetries)
                .upsert(indexRequest);
    bulkProcessor.add(updateRequest);
  }

  /**
   * Deletes the given search document.
   *
   * @param docId the ID of the document
   */
  public void deleteDocument(@Nonnull String docId) {
    final DeleteRequest deleteRequest =
        new DeleteRequest(indexConvention.getIndexName(INDEX_NAME)).id(docId);
    bulkProcessor.add(deleteRequest);
  }

  public BulkByScrollResponse deleteByQuery(@Nullable final String sourceType, @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType, @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes, @Nonnull final RelationshipFilter relationshipFilter) {
    BoolQueryBuilder finalQuery =
        buildQuery(sourceType == null ? ImmutableList.of() : ImmutableList.of(sourceType), sourceEntityFilter,
            destinationType == null ? ImmutableList.of() : ImmutableList.of(destinationType), destinationEntityFilter,
            relationshipTypes, relationshipFilter);

    return bulkProcessor.deleteByQuery(finalQuery, indexConvention.getIndexName(INDEX_NAME))
            .orElse(null);
  }
}
