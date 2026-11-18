package com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2;

import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Builds the OpenSearch 2.x kNN query map from a {@link KnnSearchRequest}.
 *
 * <p>OpenSearch 2.x uses a nested kNN query with pre-filtering (OpenSearch 2.17+) placed inside the
 * kNN block:
 *
 * <pre>{@code
 * {
 *   "size": k,
 *   "track_total_hits": false,
 *   "_source": [...],
 *   "query": {
 *     "nested": {
 *       "path": "<vectorField minus .vector>",
 *       "score_mode": "max",
 *       "query": {
 *         "knn": {
 *           "<vectorField>": {
 *             "vector": [...],
 *             "k": k,
 *             "filter": { ... }     // optional, placed inside kNN for pre-filtering
 *           }
 *         }
 *       }
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Note: the {@code vector} field is a {@code List<Float>} (not {@code float[]}) to match
 * OpenSearch serialization expectations.
 */
public final class OpenSearch2KnnQueryBuilder {

  private OpenSearch2KnnQueryBuilder() {}

  @Nonnull
  public static Map<String, Object> build(@Nonnull KnnSearchRequest req) {
    String nestedPath = deriveNestedPath(req.vectorField());

    Map<String, Object> knnParams = new HashMap<>();
    knnParams.put("vector", toFloatList(req.queryVector()));
    knnParams.put("k", req.k());
    // Map numCandidates to OpenSearch's method_parameters.ef_search for runtime exploration tuning.
    // ef_search controls how many candidates the HNSW graph explores; a higher value improves
    // recall at the cost of latency. Only meaningful when numCandidates > k.
    if (req.numCandidates() > req.k()) {
      knnParams.put("method_parameters", Map.of("ef_search", req.numCandidates()));
    }
    req.filter()
        .map(OpenSearch2KnnQueryBuilder::withRequiredOrGroups)
        .ifPresent(f -> knnParams.put("filter", f));

    Map<String, Object> query = new HashMap<>();
    query.put(
        "nested",
        Map.of(
            "path",
            nestedPath,
            "score_mode",
            "max",
            "query",
            Map.of("knn", Map.of(req.vectorField(), knnParams))));

    Map<String, Object> body = new HashMap<>();
    body.put("size", req.k());
    body.put("track_total_hits", false);
    if (!req.fieldsToFetch().isEmpty()) {
      body.put("_source", req.fieldsToFetch().toArray(new String[0]));
    }
    body.put("query", query);
    return body;
  }

  /**
   * Moves a top-level OR that sits next to required clauses into one required clause, so the root
   * filter keeps parent-document scope inside the nested kNN query.
   *
   * <p>OpenSearch 2.x decides where a nested kNN filter runs from the filter's required clauses
   * alone ({@code NestedHelper#mightMatchNestedDocs}): when a bool has any, its {@code should}
   * clauses are not looked at. A filter shaped {@code must: [latest-version clause], should:
   * [group, ...], minimum_should_match: 1} is therefore judged by the latest-version clause, whose
   * "not versioned" branch might match chunk documents. The whole filter then runs against the
   * chunks, which carry no root fields, and kNN finds no candidates. Written as {@code filter:
   * [{bool: {should, minimum_should_match}}]} it matches the same documents, and the root-field
   * clauses inside the groups keep it at parent scope. OpenSearch 3.x scopes these filters itself
   * (opensearch-project/k-NN#2990).
   */
  @Nonnull
  private static Map<String, Object> withRequiredOrGroups(@Nonnull Map<String, Object> filter) {
    if (!(filter.get("bool") instanceof Map<?, ?> bool)
        || !(bool.get("should") instanceof List<?> groups)
        || groups.isEmpty()
        || !requiresShouldMatch(bool.get("minimum_should_match"))
        || (clauses(bool.get("must")).isEmpty() && clauses(bool.get("filter")).isEmpty())) {
      return filter;
    }
    Map<String, Object> rewritten = new HashMap<>();
    bool.forEach((key, value) -> rewritten.put(String.valueOf(key), value));
    rewritten.remove("should");
    rewritten.remove("minimum_should_match");
    List<Object> required = new ArrayList<>(clauses(bool.get("filter")));
    required.add(
        Map.of(
            "bool",
            Map.of("should", groups, "minimum_should_match", bool.get("minimum_should_match"))));
    rewritten.put("filter", required);
    return Map.of("bool", rewritten);
  }

  /** A bool clause value in either JSON form: an array of clauses, or a single clause. */
  @Nonnull
  private static List<?> clauses(@Nullable Object value) {
    if (value instanceof List<?> list) {
      return list;
    }
    return value == null ? List.of() : List.of(value);
  }

  /**
   * Whether {@code minimum_should_match} makes at least one {@code should} clause mandatory.
   * Without it, {@code should} clauses next to {@code must} or {@code filter} only affect scoring,
   * and making them required would change which documents match.
   */
  private static boolean requiresShouldMatch(@Nullable Object minimumShouldMatch) {
    if (minimumShouldMatch == null) {
      return false;
    }
    try {
      return Integer.parseInt(String.valueOf(minimumShouldMatch).trim()) >= 1;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Nonnull
  private static List<Float> toFloatList(float[] vec) {
    List<Float> out = new ArrayList<>(vec.length);
    for (float v : vec) {
      out.add(v);
    }
    return out;
  }

  @Nonnull
  private static String deriveNestedPath(@Nonnull String vectorField) {
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
