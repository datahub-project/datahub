package com.linkedin.metadata.search.elasticsearch.index;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

/**
 * Reads structured-property values from one search index, a batch at a time.
 *
 * <p>Both document shapes are returned as dotted {@code structuredProperties.<child>} names: a
 * nested {@code structuredProperties} object, and dotted keys stored at the root. A versioned
 * property is nested under {@code _versioned} in {@code _source} and is flattened to the same
 * dotted leaf the mapping catalog uses. Only that object and those keys are requested. A null child
 * is absent.
 */
public class StructuredPropertyIndexFields {

  private static final String URN_FIELD = "urn";
  private static final String[] SOURCE_INCLUDES =
      new String[] {
        URN_FIELD, STRUCTURED_PROPERTY_MAPPING_FIELD, STRUCTURED_PROPERTY_MAPPING_FIELD + ".*"
      };

  private final Searcher searcher;
  private final ObjectMapper objectMapper;

  @FunctionalInterface
  public interface Searcher {
    @Nonnull
    SearchResponse search(
        @Nonnull OperationContext opContext,
        @Nonnull String indexName,
        @Nonnull SearchRequest request)
        throws IOException;
  }

  public StructuredPropertyIndexFields(@Nonnull Searcher searcher) {
    this(searcher, new ObjectMapper());
  }

  StructuredPropertyIndexFields(@Nonnull Searcher searcher, @Nonnull ObjectMapper objectMapper) {
    this.searcher = searcher;
    this.objectMapper = objectMapper;
  }

  /** Values on one document. {@code fields} contains only present structured-property children. */
  public record DocumentValues(
      @Nonnull String documentId, @Nullable String urn, @Nonnull Map<String, JsonNode> fields) {}

  /** One filtered page. {@code scrollId} is null when the page is the last. */
  public record Page(@Nonnull List<DocumentValues> documents, @Nullable String scrollId) {}

  /** One ids query for {@code documentIds}. A document that is not in the index has no entry. */
  @Nonnull
  public Map<String, DocumentValues> byDocumentId(
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nonnull Collection<String> documentIds)
      throws IOException {
    if (documentIds.isEmpty()) {
      return Map.of();
    }
    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(QueryBuilders.idsQuery().addIds(documentIds.toArray(new String[0])))
            .size(documentIds.size())
            .fetchSource(SOURCE_INCLUDES, null);
    SearchResponse response =
        searcher.search(opContext, indexName, new SearchRequest(indexName).source(source));
    Map<String, DocumentValues> byId = new LinkedHashMap<>();
    for (SearchHit hit : response.getHits().getHits()) {
      DocumentValues values = documentValues(hit);
      byId.put(values.documentId(), values);
    }
    return byId;
  }

  /**
   * One page of documents that have a {@code structuredProperties} value, in {@code urn} order.
   * Documents with no structured-property fields are not part of the query. {@code entityType}
   * limits the page to the V3 {@code _entityType} keyword; a null type leaves the query unscoped,
   * which is what a per-entity index already is.
   */
  @Nonnull
  public Page scroll(
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      int size,
      @Nullable String scrollId,
      @Nullable String entityType)
      throws IOException {
    int pageSize = Math.max(size, 1);
    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(scrollQuery(entityType))
            .size(pageSize)
            .fetchSource(SOURCE_INCLUDES, null)
            // Elasticsearch 8 rejects a sort on _id because that field has no fielddata.
            // urn is a keyword on every entity index, and one document has one urn.
            .sort(URN_FIELD, SortOrder.ASC);
    if (scrollId != null && !scrollId.isEmpty()) {
      source.searchAfter(SearchAfterWrapper.fromScrollId(scrollId).getSort());
    }
    SearchResponse response =
        searcher.search(opContext, indexName, new SearchRequest(indexName).source(source));
    SearchHit[] hits = response.getHits().getHits();
    List<DocumentValues> documents = new ArrayList<>(hits.length);
    for (SearchHit hit : hits) {
      documents.add(documentValues(hit));
    }
    return new Page(documents, SearchAfterWrapper.nextScrollId(hits, pageSize));
  }

  /**
   * Exists on {@code structuredProperties}, plus an exact {@code _entityType} term when the index
   * holds more than one entity type. The term field is mapped as keyword.
   */
  @Nonnull
  private static QueryBuilder scrollQuery(@Nullable String entityType) {
    QueryBuilder exists = QueryBuilders.existsQuery(STRUCTURED_PROPERTY_MAPPING_FIELD);
    if (entityType == null || entityType.isEmpty()) {
      return exists;
    }
    return QueryBuilders.boolQuery()
        .filter(exists)
        .filter(QueryBuilders.termQuery(MappingConstants.ENTITY_TYPE_FIELD, entityType));
  }

  @Nonnull
  private DocumentValues documentValues(@Nonnull SearchHit hit) {
    Map<String, Object> source = hit.getSourceAsMap();
    if (source == null) {
      source = Map.of();
    }
    Object urn = source.get(URN_FIELD);
    return new DocumentValues(
        hit.getId(), urn == null ? null : urn.toString(), fieldsFromSource(source, objectMapper));
  }

  /**
   * Dotted field names present in {@code source}. A nested object and dotted root keys are both
   * accepted. Nested objects, including {@code _versioned}, are walked down to the stored value.
   * Null children are omitted. A dotted root key does not replace a nested child.
   */
  @Nonnull
  static Map<String, JsonNode> fieldsFromSource(
      @Nullable Map<String, Object> source, @Nonnull ObjectMapper objectMapper) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    Map<String, JsonNode> fields = new LinkedHashMap<>();
    Object nested = source.get(STRUCTURED_PROPERTY_MAPPING_FIELD);
    if (nested instanceof Map<?, ?> children) {
      flatten(STRUCTURED_PROPERTY_MAPPING_FIELD, children, fields, objectMapper);
    }
    for (Map.Entry<String, Object> entry : source.entrySet()) {
      String key = entry.getKey();
      if (entry.getValue() == null
          || !key.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD + ".")
          || fields.containsKey(key)) {
        continue;
      }
      fields.put(key, objectMapper.valueToTree(entry.getValue()));
    }
    return fields;
  }

  /** Walks a nested object to the stored value. Arrays and scalars are that value. */
  private static void flatten(
      @Nonnull String prefix,
      @Nonnull Map<?, ?> children,
      @Nonnull Map<String, JsonNode> fields,
      @Nonnull ObjectMapper objectMapper) {
    for (Map.Entry<?, ?> entry : children.entrySet()) {
      if (!(entry.getKey() instanceof String name) || name.isEmpty() || entry.getValue() == null) {
        continue;
      }
      String path = prefix + "." + name;
      if (entry.getValue() instanceof Map<?, ?> nested) {
        flatten(path, nested, fields, objectMapper);
      } else {
        fields.put(path, objectMapper.valueToTree(entry.getValue()));
      }
    }
  }
}
