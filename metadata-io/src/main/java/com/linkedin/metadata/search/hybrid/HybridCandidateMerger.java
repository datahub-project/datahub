package com.linkedin.metadata.search.hybrid;

import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Fuses lexical and vector scores for hybrid retrieval.
 *
 * <p>Totals, facets and visible rows stay lexical-backed. Vector-only rows would need a later API
 * contract, so this merger only returns candidates that appeared in the lexical result set.
 */
public class HybridCandidateMerger {

  public static final double ABSENT_VECTOR_SCORE = 0d;

  private final HybridLexicalScoreNormalizer lexicalScoreNormalizer;
  private final HybridScoreCombiner scoreCombiner;

  public HybridCandidateMerger(
      @Nonnull final HybridLexicalScoreNormalizer lexicalScoreNormalizer,
      @Nonnull final HybridScoreCombiner scoreCombiner) {
    this.lexicalScoreNormalizer = lexicalScoreNormalizer;
    this.scoreCombiner = scoreCombiner;
  }

  @Nonnull
  public List<HybridCandidate> merge(
      @Nonnull final Map<Urn, Double> lexicalScores,
      @Nullable final Map<Urn, Double> vectorScores) {
    final Map<Urn, Double> orderedLexicalScores = new LinkedHashMap<>(lexicalScores);
    final Map<Urn, Double> safeVectorScores = vectorScores != null ? vectorScores : Map.of();

    final List<HybridCandidate> candidates = new ArrayList<>(orderedLexicalScores.size());
    for (Map.Entry<Urn, Double> lexicalEntry : orderedLexicalScores.entrySet()) {
      final double lexicalScore = lexicalEntry.getValue() != null ? lexicalEntry.getValue() : 0d;
      final double vectorScore =
          safeVectorScores.getOrDefault(lexicalEntry.getKey(), ABSENT_VECTOR_SCORE);
      candidates.add(
          scoreCombiner.combine(
              lexicalEntry.getKey(), lexicalScoreNormalizer.normalize(lexicalScore), vectorScore));
    }
    candidates.sort(Comparator.comparing(HybridCandidate::combinedScore).reversed());
    return candidates;
  }
}
