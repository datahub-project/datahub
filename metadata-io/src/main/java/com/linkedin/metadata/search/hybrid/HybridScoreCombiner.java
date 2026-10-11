package com.linkedin.metadata.search.hybrid;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/** Combines lexical relevance and normalized vector scores for hybrid retrieval. */
public class HybridScoreCombiner {

  public static final double DEFAULT_VECTOR_WEIGHT = 0.5d;

  private final double vectorWeight;

  public HybridScoreCombiner() {
    this(DEFAULT_VECTOR_WEIGHT);
  }

  public HybridScoreCombiner(final double vectorWeight) {
    if (!Double.isFinite(vectorWeight) || vectorWeight < 0d || vectorWeight > 1d) {
      throw new IllegalArgumentException("vectorWeight must be between 0 and 1");
    }
    this.vectorWeight = vectorWeight;
  }

  /**
   * @param scaledLexicalScore centered lexical score from {@link HybridLexicalScoreNormalizer}, not
   *     raw ES/OpenSearch BM25 {@code _score} and not an already-normalized {@code [0, 1]} value
   * @param vectorScore vector similarity already normalized to {@code [0, 1]}
   */
  @Nonnull
  public HybridCandidate combine(
      @Nonnull final Urn entity, final double scaledLexicalScore, final double vectorScore) {
    final double normalizedLexicalScore = sigmoid(scaledLexicalScore);
    final double normalizedVectorScore =
        Double.isFinite(vectorScore) && vectorScore >= 0d && vectorScore <= 1d ? vectorScore : 0d;
    final double combinedScore =
        (vectorWeight * normalizedVectorScore) + ((1d - vectorWeight) * normalizedLexicalScore);
    return new HybridCandidate(
        entity,
        scaledLexicalScore,
        vectorScore,
        normalizedLexicalScore,
        normalizedVectorScore,
        combinedScore);
  }

  public double getVectorWeight() {
    return vectorWeight;
  }

  static double sigmoid(final double score) {
    if (score >= 0d) {
      final double exponent = Math.exp(-score);
      return 1d / (1d + exponent);
    }
    final double exponent = Math.exp(score);
    return exponent / (1d + exponent);
  }
}
