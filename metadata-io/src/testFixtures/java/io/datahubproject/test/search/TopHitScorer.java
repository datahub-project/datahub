package io.datahubproject.test.search;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Probe checks that sit beside nDCG and are never averaged with it. */
public final class TopHitScorer {
  private TopHitScorer() {}

  public static void assertProbe(@Nonnull JsonNode judgment, @Nonnull RankedSearchResult result) {
    List<String> failures = new ArrayList<>();
    JsonNode exact = judgment.get("exactTopHit");
    if (exact != null && !exact.isNull()) {
      if (result.getHits().isEmpty() || !exact.asText().equals(result.getHits().get(0).getUrn())) {
        failures.add(
            "expected top hit "
                + exact.asText()
                + " but was "
                + (result.getHits().isEmpty() ? "<none>" : result.getHits().get(0).getUrn()));
      }
    }
    JsonNode expectedOrder = judgment.get("expectedOrder");
    if (expectedOrder != null && expectedOrder.isArray()) {
      List<String> expected = new ArrayList<>();
      expectedOrder.forEach(node -> expected.add(node.asText()));
      List<String> actual = new ArrayList<>();
      result.getHits().forEach(hit -> actual.add(hit.getUrn()));
      if (!expected.equals(actual)) {
        failures.add("expected order " + expected + " but was " + actual);
      }
    }
    JsonNode ordered = judgment.get("orderedContains");
    if (ordered != null && ordered.isArray()) {
      for (int i = 0; i < ordered.size(); i++) {
        String expected = ordered.get(i).asText();
        if (result.getHits().size() <= i || !result.getHits().get(i).getUrn().contains(expected)) {
          failures.add(
              "hit "
                  + i
                  + " should contain "
                  + expected
                  + " but was "
                  + (result.getHits().size() <= i ? "<none>" : result.getHits().get(i).getUrn()));
        }
      }
    }
    JsonNode contained = judgment.get("containsInTop");
    if (contained != null && !contained.isNull()) {
      int window = contained.path("n").asInt(0);
      JsonNode values = contained.path("values");
      if (values.isArray()) {
        boolean[] used = new boolean[Math.min(window, result.getHits().size())];
        for (JsonNode expectedNode : values) {
          String expected = expectedNode.asText();
          boolean found = false;
          for (int i = 0; i < used.length; i++) {
            if (!used[i] && result.getHits().get(i).getUrn().contains(expected)) {
              used[i] = true;
              found = true;
              break;
            }
          }
          if (!found) {
            failures.add("top " + window + " should contain " + expected);
          }
        }
      }
    }
    JsonNode present = judgment.get("present");
    if (present != null && present.isArray()) {
      for (JsonNode expected : present) {
        String value = expected.asText();
        boolean found = false;
        for (RankedSearchHit hit : result.getHits()) {
          if (hit.getUrn().contains(value)) {
            found = true;
            break;
          }
        }
        if (!found) {
          failures.add("results should contain " + value);
        }
      }
    }
    if (judgment.path("expectNoHits").asBoolean(false) && !result.getHits().isEmpty()) {
      failures.add("expected no hits but found " + result.getHits().size());
    }
    if (judgment.path("scoresStrictlyDescending").asBoolean(false)) {
      if (result.getHits().size() < 2
          || !(result.getHits().get(0).getScore() > result.getHits().get(1).getScore())) {
        failures.add("expected the first score to be strictly greater than the second");
      }
    }
    JsonNode scoreRatioAtMost = judgment.get("scoreRatioAtMost");
    if (scoreRatioAtMost != null && !scoreRatioAtMost.isNull()) {
      RankedSearchHit numerator = hit(result, scoreRatioAtMost.path("numerator").asText());
      RankedSearchHit denominator = hit(result, scoreRatioAtMost.path("denominator").asText());
      double max = scoreRatioAtMost.path("max").asDouble();
      if (numerator == null
          || denominator == null
          || denominator.getScore() <= 0.0
          || numerator.getScore() / denominator.getScore() > max) {
        failures.add(
            "score ratio "
                + scoreRatioAtMost.path("numerator").asText()
                + "/"
                + scoreRatioAtMost.path("denominator").asText()
                + " should be at most "
                + max);
      }
    }
    JsonNode matched = judgment.get("matchedFieldContains");
    if (matched != null && !matched.isNull()) {
      String value = matched.path("value").asText();
      int hits = matched.path("hits").asInt(1);
      for (int i = 0; i < hits; i++) {
        if (result.getHits().size() <= i
            || result.getHits().get(i).getMatchedFields().stream()
                .noneMatch(field -> field.contains(value))) {
          failures.add("hit " + i + " matched fields should contain " + value);
        }
      }
    }
    JsonNode rankAbove = judgment.get("rankAbove");
    if (rankAbove != null && !rankAbove.isNull()) {
      int anchor = indexOf(result, rankAbove.path("urn").asText());
      for (JsonNode above : rankAbove.path("above")) {
        int other = indexOf(result, above.asText());
        if (anchor < 0 || other < 0 || anchor >= other) {
          failures.add(rankAbove.path("urn").asText() + " should rank above " + above.asText());
        }
      }
    }
    if (!failures.isEmpty()) {
      String hits =
          result.getHits().stream()
              .map(
                  hit ->
                      hit.getUrn()
                          + " score="
                          + hit.getScore()
                          + " matched="
                          + hit.getMatchedFields())
              .reduce((left, right) -> left + " | " + right)
              .orElse("<none>");
      throw new AssertionError(
          judgment.path("id").asText()
              + ": "
              + String.join("; ", failures)
              + " hits=["
              + hits
              + "]");
    }
  }

  private static int indexOf(RankedSearchResult result, String urn) {
    for (int i = 0; i < result.getHits().size(); i++) {
      if (urn.equals(result.getHits().get(i).getUrn())) {
        return i;
      }
    }
    return -1;
  }

  @Nullable
  private static RankedSearchHit hit(RankedSearchResult result, String urn) {
    for (RankedSearchHit hit : result.getHits()) {
      if (urn.equals(hit.getUrn())) {
        return hit;
      }
    }
    return null;
  }
}
