package com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2;

import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

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
    req.filter().ifPresent(f -> knnParams.put("filter", f));

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
