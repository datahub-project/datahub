package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.READ_ONLY_LOG;
import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_GRAPH_WRITE_VERSION;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;
import static com.linkedin.metadata.graph.elastic.utils.GraphQueryUtils.buildQuery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
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
import org.opensearch.script.ScriptType;

@Slf4j
@RequiredArgsConstructor
public class ESGraphWriteDAO {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * When the edge doc exists and its graphWriteVersion is strictly greater than the incoming write,
   * skip the update. Otherwise apply the full document. Creates via upsert when the doc is missing.
   *
   * <p>Legacy docs without {@code graphWriteVersion} are treated as unversioned: any versioned
   * incoming write applies (and stamps the field). Must stay in sync with {@link
   * #shouldNoopConditionalGraphUpsert}.
   */
  private static final String CONDITIONAL_GRAPH_UPSERT_SCRIPT =
      "boolean hasExistingVersion = ctx._source.containsKey('"
          + EDGE_FIELD_GRAPH_WRITE_VERSION
          + "') && ctx._source."
          + EDGE_FIELD_GRAPH_WRITE_VERSION
          + " != null; "
          + "if (hasExistingVersion && params.graphWriteVersion != null "
          + "&& ctx._source."
          + EDGE_FIELD_GRAPH_WRITE_VERSION
          + " > params.graphWriteVersion) { "
          + "  ctx.op = 'noop'; "
          + "} else { "
          + "  ctx._source.putAll(params.doc); "
          + "}";

  /**
   * Java mirror of {@link #CONDITIONAL_GRAPH_UPSERT_SCRIPT} for unit tests and docs.
   *
   * @param existingVersion version on the stored edge doc, or null if the field is absent
   * @param incomingVersion version on the write; null means unconditional apply (caller uses
   *     non-script path)
   * @return true when the scripted upsert should no-op
   */
  static boolean shouldNoopConditionalGraphUpsert(
      @Nullable Long existingVersion, @Nullable Long incomingVersion) {
    return existingVersion != null && incomingVersion != null && existingVersion > incomingVersion;
  }

  private final IndexConvention indexConvention;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;
  private final GraphQueryConfiguration graphQueryConfiguration;
  private boolean canWrite = true;

  public void setWritable(boolean writable) {
    canWrite = writable;
  }

  /**
   * Updates or inserts the given search document with optional graph write-version fencing.
   *
   * @param document the document to update / insert
   * @param docId the ID of the document
   * @param graphWriteVersion aspect version for this write; null skips conditional script / fence
   */
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String docId,
      @Nonnull String document,
      @Nullable Long graphWriteVersion) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    final UpdateRequest updateRequest;
    if (graphWriteVersion != null) {
      Map<String, Object> docMap;
      try {
        docMap = OBJECT_MAPPER.readValue(document, new TypeReference<Map<String, Object>>() {});
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse graph edge document for conditional upsert", e);
      }
      Map<String, Object> scriptParams = new HashMap<>();
      scriptParams.put("graphWriteVersion", graphWriteVersion);
      scriptParams.put("doc", docMap);
      Script script =
          new Script(ScriptType.INLINE, "painless", CONDITIONAL_GRAPH_UPSERT_SCRIPT, scriptParams);
      updateRequest =
          new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId)
              .detectNoop(false)
              .scriptedUpsert(true)
              .script(script)
              .upsert(document, XContentType.JSON)
              .retryOnConflict(numRetries);
    } else {
      updateRequest =
          new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId)
              .detectNoop(false)
              .docAsUpsert(true)
              .doc(document, XContentType.JSON)
              .retryOnConflict(numRetries);
    }
    // Route by docId (hash of source + relationshipType + destination + lifecycleOwner)
    // so remove+add pairs on the same edge land on the same bulk processor thread.
    // Otherwise same-docId writes race on OpenSearch's seqNo and retryOnConflict
    // cannot converge — the losing write is silently dropped.
    GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, graphWriteVersion, updateRequest);
    bulkProcessor.add(opContext, docId, updateRequest);
  }

  /** Compatibility overload without graph write version. */
  public void upsertDocument(
      @Nonnull OperationContext opContext, @Nonnull String docId, @Nonnull String document) {
    upsertDocument(opContext, docId, document, null);
  }

  /**
   * Deletes the given search document.
   *
   * @param docId the ID of the document
   * @param graphWriteVersion aspect version for fence recording; null skips fencing
   */
  public void deleteDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String docId,
      @Nullable Long graphWriteVersion) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    final DeleteRequest deleteRequest =
        new DeleteRequest(indexConvention.getIndexName(INDEX_NAME)).id(docId);
    GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, graphWriteVersion, deleteRequest);
    // Route by docId — see upsertDocument above.
    bulkProcessor.add(opContext, docId, deleteRequest);
  }

  /** Compatibility overload without graph write version. */
  public void deleteDocument(@Nonnull OperationContext opContext, @Nonnull String docId) {
    deleteDocument(opContext, docId, null);
  }

  @Nullable
  public BulkByScrollResponse deleteByQuery(
      @Nonnull final OperationContext opContext, @Nonnull final GraphFilters graphFilters) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return null;
    }
    return deleteByQuery(opContext, graphFilters, null);
  }

  @Nullable
  public BulkByScrollResponse deleteByQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      String lifecycleOwner) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return null;
    }
    BoolQueryBuilder finalQuery =
        buildQuery(opContext, graphQueryConfiguration, graphFilters, lifecycleOwner);

    return bulkProcessor
        .deleteByQuery(opContext, finalQuery, indexConvention.getIndexName(INDEX_NAME))
        .orElse(null);
  }

  @Nullable
  public BulkByScrollResponse updateByQuery(
      @Nonnull OperationContext opContext,
      @Nonnull Script script,
      @Nonnull final QueryBuilder query) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return null;
    }
    return bulkProcessor
        .updateByQuery(opContext, script, query, indexConvention.getIndexName(INDEX_NAME))
        .orElse(null);
  }
}
