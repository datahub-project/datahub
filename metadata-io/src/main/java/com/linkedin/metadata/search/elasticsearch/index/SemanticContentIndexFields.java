package com.linkedin.metadata.search.elasticsearch.index;

import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchDocumentProjector;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Reads minimal {@code semanticContent} write-unit key fields from a Search V3 entity index a batch
 * at a time.
 *
 * <p>Requests only root scalar/marker fields and per-profile state/hash/marker fields — never
 * {@code chunks} arrays or embedding vectors.
 */
public class SemanticContentIndexFields {

  private static final String URN_FIELD = "urn";
  private static final String REFRESH_MARKER_FIELD = "refreshMarker";
  private static final String SOURCES_FIELD = "sources";
  private static final String CREATED_TIME_FIELD = "createdTime";
  private static final String VERSION_FIELD = "version";

  private static final List<String> PROFILE_SCALAR_FIELDS =
      List.of(
          "vectorState",
          "current",
          "contentHash",
          "sourceTextSha256",
          "totalChunks",
          "generatedAt",
          REFRESH_MARKER_FIELD);

  private final Searcher searcher;

  @FunctionalInterface
  public interface Searcher {
    @Nonnull
    SearchResponse search(
        @Nonnull OperationContext opContext,
        @Nonnull String indexName,
        @Nonnull SearchRequest request)
        throws IOException;
  }

  public SemanticContentIndexFields(@Nonnull Searcher searcher) {
    this.searcher = Objects.requireNonNull(searcher, "searcher");
  }

  /** Minimal per-profile write-unit fingerprint stored on a V3 document. */
  public record ProfileValues(
      @Nullable String vectorState,
      @Nullable Boolean current,
      @Nullable String contentHash,
      @Nullable String sourceTextSha256,
      @Nullable Integer totalChunks,
      @Nullable Long generatedAt,
      @Nullable SemanticRefreshMarker refreshMarker) {}

  /** Minimal root + per-profile {@code semanticContent} fingerprint stored on a V3 document. */
  public record DocumentValues(
      @Nonnull String documentId,
      @Nullable String urn,
      boolean tombstone,
      @Nullable String skipReason,
      @Nullable SemanticRefreshMarker refreshMarker,
      @Nonnull Map<String, ProfileValues> profiles) {}

  /**
   * Executes one {@code idsQuery} for {@code documentIds}, fetching only root and configured
   * profile scalar/marker fields. Documents absent from the index are omitted from the returned
   * map.
   */
  @Nonnull
  public Map<String, DocumentValues> byDocumentId(
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nonnull Collection<String> documentIds,
      @Nonnull Collection<String> modelKeys)
      throws IOException {
    if (documentIds.isEmpty()) {
      return Map.of();
    }
    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(QueryBuilders.idsQuery().addIds(documentIds.toArray(new String[0])))
            .size(documentIds.size())
            .fetchSource(sourceIncludes(modelKeys), null);
    SearchResponse response =
        searcher.search(opContext, indexName, new SearchRequest(indexName).source(source));
    Map<String, DocumentValues> byId = new LinkedHashMap<>();
    for (SearchHit hit : response.getHits().getHits()) {
      DocumentValues values = documentValues(hit, modelKeys);
      byId.put(values.documentId(), values);
    }
    return byId;
  }

  @Nonnull
  static String[] sourceIncludes(@Nonnull Collection<String> modelKeys) {
    List<String> includes = new ArrayList<>(4 + modelKeys.size() * PROFILE_SCALAR_FIELDS.size());
    includes.add(URN_FIELD);
    includes.add(V3SearchDocumentProjector.TOMBSTONE_FIELD);
    includes.add(SemanticEmbeddingMappings.SKIP_REASON_FIELD);
    includes.add(REFRESH_MARKER_FIELD);
    for (String modelKey : modelKeys) {
      String prefix = SemanticEmbeddingMappings.EMBEDDINGS_FIELD + "." + modelKey + ".";
      for (String field : PROFILE_SCALAR_FIELDS) {
        includes.add(prefix + field);
      }
    }
    return includes.toArray(new String[0]);
  }

  @Nonnull
  static DocumentValues documentValues(
      @Nonnull SearchHit hit, @Nonnull Collection<String> modelKeys) {
    Map<String, Object> source = hit.getSourceAsMap();
    if (source == null) {
      source = Map.of();
    }
    String urn = stringOrNull(source.get(URN_FIELD));
    boolean tombstone =
        Boolean.TRUE.equals(booleanOrNull(source.get(V3SearchDocumentProjector.TOMBSTONE_FIELD)));
    String skipReason = stringOrNull(source.get(SemanticEmbeddingMappings.SKIP_REASON_FIELD));
    SemanticRefreshMarker refreshMarker = parseRefreshMarker(source.get(REFRESH_MARKER_FIELD));

    Map<String, ProfileValues> profiles = new LinkedHashMap<>();
    Object rawEmbeddings = source.get(SemanticEmbeddingMappings.EMBEDDINGS_FIELD);
    if (rawEmbeddings instanceof Map<?, ?> embeddingsMap) {
      for (String modelKey : modelKeys) {
        Object rawProfile = embeddingsMap.get(modelKey);
        if (rawProfile instanceof Map<?, ?> profileMap) {
          profiles.put(modelKey, parseProfile(profileMap));
        }
      }
    }
    return new DocumentValues(
        hit.getId(),
        urn,
        tombstone,
        skipReason,
        refreshMarker,
        Collections.unmodifiableMap(profiles));
  }

  @Nonnull
  private static ProfileValues parseProfile(@Nonnull Map<?, ?> profileMap) {
    return new ProfileValues(
        stringOrNull(profileMap.get("vectorState")),
        booleanOrNull(profileMap.get("current")),
        stringOrNull(profileMap.get("contentHash")),
        stringOrNull(profileMap.get("sourceTextSha256")),
        integerOrNull(profileMap.get("totalChunks")),
        longOrNull(profileMap.get("generatedAt")),
        parseRefreshMarker(profileMap.get(REFRESH_MARKER_FIELD)));
  }

  @Nullable
  static SemanticRefreshMarker parseRefreshMarker(@Nullable Object raw) {
    if (!(raw instanceof Map<?, ?> markerMap)) {
      return null;
    }
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    Object rawSources = markerMap.get(SOURCES_FIELD);
    if (rawSources instanceof Map<?, ?> sourcesMap) {
      for (Map.Entry<?, ?> entry : sourcesMap.entrySet()) {
        if (!(entry.getKey() instanceof String key)
            || !(entry.getValue() instanceof Map<?, ?> tokenMap)) {
          continue;
        }
        Long createdTime = longOrNull(tokenMap.get(CREATED_TIME_FIELD));
        Long version = longOrNull(tokenMap.get(VERSION_FIELD));
        if (createdTime != null && version != null) {
          sources.put(
              key, new SemanticSourceToken().setCreatedTime(createdTime).setVersion(version));
        }
      }
    }
    return new SemanticRefreshMarker().setSources(sources);
  }

  @Nullable
  private static String stringOrNull(@Nullable Object value) {
    return value == null ? null : value.toString();
  }

  @Nullable
  private static Boolean booleanOrNull(@Nullable Object value) {
    if (value instanceof Boolean b) {
      return b;
    }
    if (value instanceof String s) {
      if ("true".equalsIgnoreCase(s)) {
        return Boolean.TRUE;
      }
      if ("false".equalsIgnoreCase(s)) {
        return Boolean.FALSE;
      }
    }
    return null;
  }

  @Nullable
  private static Integer integerOrNull(@Nullable Object value) {
    if (value instanceof Number n) {
      return n.intValue();
    }
    return null;
  }

  @Nullable
  private static Long longOrNull(@Nullable Object value) {
    if (value instanceof Number n) {
      return n.longValue();
    }
    return null;
  }
}
