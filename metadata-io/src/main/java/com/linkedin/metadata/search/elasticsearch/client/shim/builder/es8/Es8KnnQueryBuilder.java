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
    // A filter inside the kNN clause is applied to the parent documents of the nested vectors, so
    // it pre-filters on root fields (entityType, platform, urn and the like): the nearest-neighbour
    // search only considers matching documents instead of dropping non-matches from the top k.
    req.filter().ifPresent(f -> knnInner.put("filter", f));

    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("path", nestedPath);
    nested.put("score_mode", "max");
    nested.put("query", Map.of("knn", knnInner));

    List<Map<String, Object>> must = new ArrayList<>();
    must.add(Map.of("nested", nested));

    Map<String, Object> bool = new LinkedHashMap<>();
    bool.put("must", must);

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("size", req.k());
    body.put("track_total_hits", false);
    if (!req.fieldsToFetch().isEmpty()) {
      body.put("_source", Map.of("includes", req.fieldsToFetch()));
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
