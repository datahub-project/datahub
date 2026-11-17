package com.linkedin.metadata.search.hybrid;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Converts engine-specific kNN {@code _score} values into a common {@code [0, 1]} similarity.
 *
 * <p>Elasticsearch emits normalized cosine, L2, and unit-vector dot-product scores. OpenSearch
 * cosine (faiss on 2.19+, Lucene on any supported version) and L2 scores are already in {@code [0,
 * 1]}, while inner-product scores must first be inverted back to the dot product.
 */
public final class HybridVectorScoreNormalizer {

  private static final Set<String> COSINE_METRICS = Set.of("cosine", "cosinesimil");
  private static final Set<String> L2_METRICS = Set.of("l2", "l2_norm");
  private static final Set<String> DOT_PRODUCT_METRICS =
      Set.of("dot_product", "dotproduct", "innerproduct");

  private final SearchEngineType engineType;
  private final String metric;

  public HybridVectorScoreNormalizer(
      @Nonnull final SearchEngineType engineType, @Nonnull final String metric) {
    if (!engineType.requiresEs8JavaClient() && !engineType.requiresOpenSearchClient()) {
      throw new IllegalArgumentException(
          "Hybrid vector scoring requires Elasticsearch 8+ or OpenSearch 2+");
    }
    final String normalizedMetric = metric.trim().toLowerCase(Locale.ROOT);
    if (!isSupportedMetric(normalizedMetric)) {
      throw new IllegalArgumentException("Unsupported hybrid vector metric: " + metric);
    }
    this.engineType = engineType;
    this.metric = normalizedMetric;
  }

  public double normalize(final double rawScore) {
    if (!Double.isFinite(rawScore) || rawScore < 0d) {
      return 0d;
    }
    if (engineType.requiresOpenSearchClient()) {
      // Cosine needs no engine-specific mapping: every combination hybrid read allows already
      // returns [0, 1]. OpenSearch 2.19.0 changed faiss cosine from "1 + cosine" to a direct
      // [0, 1] scale (opensearch-project/k-NN#2561), and the Lucene engine has always emitted
      // (1 + cosine) / 2. Halving again would compress vector scores to [0, 0.5] and under-weight
      // them relative to Elasticsearch. Hybrid read requires OpenSearch 3.5+, so the pre-2.19
      // faiss/nmslib "1 + cosine" scale never reaches this code.
      if (DOT_PRODUCT_METRICS.contains(metric)) {
        return normalizeOpenSearchInnerProduct(rawScore);
      }
    }
    return clamp(rawScore);
  }

  public static boolean isSupportedMetric(@Nonnull final String metric) {
    final String normalizedMetric = metric.trim().toLowerCase(Locale.ROOT);
    return COSINE_METRICS.contains(normalizedMetric)
        || L2_METRICS.contains(normalizedMetric)
        || DOT_PRODUCT_METRICS.contains(normalizedMetric);
  }

  private static double normalizeOpenSearchInnerProduct(final double rawScore) {
    if (rawScore == 0d) {
      return 0d;
    }
    final double dotProduct = rawScore > 1d ? rawScore - 1d : 1d - (1d / rawScore);
    return clamp((dotProduct + 1d) / 2d);
  }

  private static double clamp(final double score) {
    return Math.max(0d, Math.min(1d, score));
  }
}
