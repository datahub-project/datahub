package com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8;

import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public final class Es8KnnQueryBuilder {

  private Es8KnnQueryBuilder() {}

  @Nonnull
  public static Map<String, Object> build(@Nonnull KnnSearchRequest req) {
    String nestedPath = deriveNestedPath(req.vectorField());

    Map<String, Object> knnInner = new LinkedHashMap<>();
    knnInner.put("field", req.vectorField());
    knnInner.put("query_vector", req.queryVector());
    knnInner.put("k", req.k());
    knnInner.put("num_candidates", req.numCandidates());
    // NOTE: kNN-clause `filter` would run in nested chunk context — only useful for chunk-level
    // filters. Root-level filters (entityType, platform, urn, etc.) go in bool.filter below.

    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("path", nestedPath);
    nested.put("score_mode", "max");
    nested.put("query", Map.of("knn", knnInner));

    List<Map<String, Object>> must = new ArrayList<>();
    must.add(Map.of("nested", nested));

    Map<String, Object> bool = new LinkedHashMap<>();
    bool.put("must", must);
    // Root-level filter placement — works on root document fields (entityType, platform, urn,
    // etc.).
    // Note: this is post-filter (after kNN candidates returned). Pagination math in
    // SemanticEntitySearchService uses 1.2x oversampling; for restrictive filters this may
    // be slightly insufficient. A future improvement could use ES 8's top-level knn query
    // clause with a separate filter context, but that requires restructuring the nested-vector
    // layout.
    req.filter().ifPresent(f -> bool.put("filter", List.of(f)));

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("size", req.k());
    body.put("track_total_hits", false);
    if (!req.fieldsToFetch().isEmpty()) {
      body.put("_source", req.fieldsToFetch());
    }
    body.put("query", Map.of("bool", bool));
    return body;
  }

  private static String deriveNestedPath(String vectorField) {
    if (!vectorField.endsWith(".vector")) {
      throw new IllegalArgumentException(
          "Expected vectorField to end with .vector; got: " + vectorField);
    }
    String prefix = vectorField.substring(0, vectorField.length() - ".vector".length());
    if (prefix.isEmpty()) {
      throw new IllegalArgumentException(
          "Expected vectorField to have a non-empty nested path prefix; got: " + vectorField);
    }
    return prefix;
  }
}
