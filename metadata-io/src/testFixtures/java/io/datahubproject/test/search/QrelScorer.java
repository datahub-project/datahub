package io.datahubproject.test.search;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Graded retrieval metrics. Relevance at or below zero is nonrelevant, and a hit with no grade is
 * nonrelevant. nDCG@10 is the benchmark gate. Reciprocal rank and precision are diagnostics.
 */
public final class QrelScorer {
  private QrelScorer() {}

  public static double gain(int relevance) {
    if (relevance <= 0) {
      return 0.0;
    }
    return Math.scalb(1.0, relevance) - 1.0;
  }

  public static double ndcgAt10(
      @Nonnull List<String> rankedUrns, @Nonnull Map<String, Integer> grades) {
    return ndcgAt(rankedUrns, grades, 10);
  }

  public static double ndcgAt(
      @Nonnull List<String> rankedUrns, @Nonnull Map<String, Integer> grades, int k) {
    double ideal = idealDcg(grades, k);
    if (ideal == 0.0) {
      return 0.0;
    }
    return dcg(rankedUrns, grades, k) / ideal;
  }

  public static double reciprocalRank(
      @Nonnull List<String> rankedUrns, @Nonnull Map<String, Integer> grades) {
    for (int i = 0; i < rankedUrns.size(); i++) {
      if (grades.getOrDefault(rankedUrns.get(i), 0) > 0) {
        return 1.0 / (i + 1);
      }
    }
    return 0.0;
  }

  public static double precisionAt(
      @Nonnull List<String> rankedUrns, @Nonnull Map<String, Integer> grades, int k) {
    if (k <= 0) {
      return 0.0;
    }
    int limit = Math.min(k, rankedUrns.size());
    int relevant = 0;
    for (int i = 0; i < limit; i++) {
      if (grades.getOrDefault(rankedUrns.get(i), 0) > 0) {
        relevant++;
      }
    }
    return relevant / (double) k;
  }

  public static double mean(@Nonnull List<Double> values) {
    if (values.isEmpty()) {
      return 0.0;
    }
    double sum = 0.0;
    for (double value : values) {
      sum += value;
    }
    return sum / values.size();
  }

  private static double dcg(List<String> rankedUrns, Map<String, Integer> grades, int k) {
    double sum = 0.0;
    int limit = Math.min(k, rankedUrns.size());
    for (int i = 0; i < limit; i++) {
      int relevance = grades.getOrDefault(rankedUrns.get(i), 0);
      sum += gain(relevance) / log2(i + 2);
    }
    return sum;
  }

  private static double idealDcg(Map<String, Integer> grades, int k) {
    List<Integer> relevant =
        grades.values().stream()
            .filter(relevance -> relevance > 0)
            .sorted(Comparator.reverseOrder())
            .limit(k)
            .toList();
    double sum = 0.0;
    for (int i = 0; i < relevant.size(); i++) {
      sum += gain(relevant.get(i)) / log2(i + 2);
    }
    return sum;
  }

  private static double log2(int value) {
    return Math.log(value) / Math.log(2);
  }
}
