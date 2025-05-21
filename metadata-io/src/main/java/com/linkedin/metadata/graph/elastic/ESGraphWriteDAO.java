package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.buildQuery;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;

import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.script.Script;

@Slf4j
@RequiredArgsConstructor
public class ESGraphWriteDAO {
  private final IndexConvention indexConvention;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;
  private final GraphQueryConfiguration graphQueryConfiguration;

  /**
   * Updates or inserts the given search document.
   *
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String docId, @Nonnull String document) {
    final UpdateRequest updateRequest =
        new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(numRetries);
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

  public BulkByScrollResponse deleteByQuery(
      @Nonnull final OperationContext opContext, @Nonnull final GraphFilters graphFilters) {
    return deleteByQuery(opContext, graphFilters, null);
  }

  public BulkByScrollResponse deleteByQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      String lifecycleOwner) {
    BoolQueryBuilder finalQuery =
        buildQuery(opContext, graphQueryConfiguration, graphFilters, lifecycleOwner);

    return bulkProcessor
        .deleteByQuery(finalQuery, indexConvention.getIndexName(INDEX_NAME))
        .orElse(null);
  }

  @Nullable
  public BulkByScrollResponse updateByQuery(
      @Nonnull Script script, @Nonnull final QueryBuilder query) {
    return bulkProcessor
        .updateByQuery(script, query, indexConvention.getIndexName(INDEX_NAME))
        .orElse(null);
  }
}
