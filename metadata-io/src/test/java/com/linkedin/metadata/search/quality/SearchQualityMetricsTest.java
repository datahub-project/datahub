package com.linkedin.metadata.search.quality;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datahubproject.test.search.QrelScorer;
import io.datahubproject.test.search.RankedSearchHit;
import io.datahubproject.test.search.RankedSearchResult;
import io.datahubproject.test.search.SearchQualityPack;
import io.datahubproject.test.search.SearchQualityReport;
import io.datahubproject.test.search.TopHitScorer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class SearchQualityMetricsTest {

  @Test
  public void testGainTreatsNonpositiveGradesAsNonrelevant() {
    assertEquals(QrelScorer.gain(0), 0.0);
    assertEquals(QrelScorer.gain(-1), 0.0);
    assertEquals(QrelScorer.gain(1), 1.0);
    assertEquals(QrelScorer.gain(4), 15.0);
  }

  @Test
  public void testPerfectRankingScoresOneAndUnjudgedHitsScoreAsNonrelevant() {
    Map<String, Integer> grades = Map.of("a", 3, "b", 1, "c", 0);
    assertEquals(QrelScorer.ndcgAt10(List.of("a", "b"), grades), 1.0);
    double displaced = QrelScorer.ndcgAt10(List.of("a", "unjudged", "b"), grades);
    assertTrue(displaced < 1.0);
    assertTrue(displaced > 0.0);
    assertEquals(QrelScorer.reciprocalRank(List.of("c", "a"), grades), 0.5);
    assertEquals(QrelScorer.precisionAt(List.of("a", "unjudged"), grades, 2), 0.5);
  }

  @Test
  public void testEmptyIdealIsZero() {
    assertEquals(QrelScorer.ndcgAt10(List.of("a"), Map.of("a", 0)), 0.0);
    assertEquals(QrelScorer.ndcgAt10(List.of("a"), Map.of("a", -1)), 0.0);
  }

  @Test
  public void testContainsInTopAcceptsDashboardFirstAndTableFirst() {
    ObjectNode judgment = containsInTop(3, "pet_profiles", "pet_profiles");
    TopHitScorer.assertProbe(
        judgment,
        hits(
            "urn:li:dashboard:(looker,dashboards.14)",
            "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.adoption.pet_profiles,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)"));
    TopHitScorer.assertProbe(
        judgment,
        hits(
            "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.adoption.pet_profiles,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)",
            "urn:li:dashboard:(looker,dashboards.14)"));
  }

  @Test
  public void testContainsInTopRejectsATableOutsideTheWindow() {
    ObjectNode judgment = containsInTop(3, "pet_profiles", "pet_profiles");
    try {
      TopHitScorer.assertProbe(
          judgment,
          hits(
              "urn:li:dashboard:(looker,dashboards.14)",
              "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.adoption.pet_profiles,PROD)",
              "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.adoption.pet_details,PROD)",
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)"));
      throw new AssertionError("expected the second pet_profiles table to fall outside the top 3");
    } catch (AssertionError failure) {
      assertTrue(failure.getMessage().contains("top 3 should contain pet_profiles"));
    }
  }

  @Test
  public void testTopHitProbe() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode judgment = mapper.createObjectNode();
    judgment.put("id", "probe");
    judgment.put("exactTopHit", "urn:li:dataset:expected");
    RankedSearchResult hit =
        new RankedSearchResult(
            "probe",
            "keyword",
            "q",
            List.of(card("urn:li:dataset:expected"), card("urn:li:dataset:other")));
    TopHitScorer.assertProbe(judgment, hit);
  }

  @Test
  public void testScoreRatioProbeRejectsDoubleScore() {
    ObjectNode judgment = new ObjectMapper().createObjectNode();
    judgment.put("id", "probe");
    ObjectNode ratio = judgment.putObject("scoreRatioAtMost");
    ratio.put("numerator", "urn:li:dataset:name-and-qualified");
    ratio.put("denominator", "urn:li:dataset:name-only");
    ratio.put("max", 1.02);
    try {
      TopHitScorer.assertProbe(
          judgment,
          new RankedSearchResult(
              "probe",
              "keyword",
              "q",
              List.of(
                  card("urn:li:dataset:name-and-qualified", 2.0),
                  card("urn:li:dataset:name-only", 1.0))));
      throw new AssertionError("expected the doubled score to fail");
    } catch (AssertionError failure) {
      assertTrue(failure.getMessage().contains("score ratio"));
    }
  }

  @Test
  public void testReportWritesWorstQueryFirstWithMissesAndProbeFailure() throws Exception {
    Path dir = Files.createTempDirectory("search-quality-report");
    String previous = System.getProperty(SearchQualityReport.DIRECTORY_PROPERTY);
    System.setProperty(SearchQualityReport.DIRECTORY_PROPERTY, dir.toString());
    try {
      SearchQualityReport.QueryScore perfect =
          SearchQualityReport.query(
              judgment("perfect", "good query", "benchmark", Map.of("urn:relevant", 3)),
              new RankedSearchResult(
                  "perfect", "keyword", "good query", List.of(card("urn:relevant"))));
      SearchQualityReport.QueryScore miss =
          SearchQualityReport.query(
              judgment("miss", "bad query", "benchmark", Map.of("urn:expected", 3)),
              new RankedSearchResult(
                  "miss", "keyword", "bad query", List.of(card("urn:unjudged"))));
      ObjectNode probe = new ObjectMapper().createObjectNode();
      probe.put("id", "probe");
      probe.put("query", "q");
      probe.put("exactTopHit", "urn:li:dataset:expected");
      RankedSearchResult wrong =
          new RankedSearchResult("probe", "keyword", "q", List.of(card("urn:li:dataset:other")));
      Path written =
          SearchQualityReport.write(
              "elasticsearch",
              "",
              "benchmarkNdcgAt10",
              0.9,
              List.of(perfect, miss),
              List.of(SearchQualityReport.probe(probe, wrong)));
      JsonNode report = new ObjectMapper().readTree(written.toFile());
      assertEquals(written.getFileName().toString(), "elasticsearch-benchmarkNdcgAt10.json");
      assertEquals(report.path("engine").asText(), "elasticsearch");
      assertEquals(report.path("metric").asText(), "benchmarkNdcgAt10");
      assertEquals(report.path("queryCount").asInt(), 2);
      assertEquals(report.path("meanNdcgAt10").asDouble(), 0.5);
      assertEquals(report.path("floor").asDouble(), 0.9);
      assertFalse(report.path("passed").asBoolean());
      assertEquals(report.path("queries").get(0).path("id").asText(), "miss");
      assertEquals(report.path("queries").get(0).path("ndcgAt10").asDouble(), 0.0);
      assertTrue(report.path("queries").get(0).path("hits").get(0).path("relevance").isNull());
      assertEquals(
          report.path("queries").get(0).path("missed").get(0).path("urn").asText(), "urn:expected");
      assertEquals(
          report.path("queries").get(0).path("missed").get(0).path("relevance").asInt(), 3);
      assertEquals(report.path("queries").get(1).path("id").asText(), "perfect");
      assertEquals(report.path("queries").get(1).path("ndcgAt10").asDouble(), 1.0);
      assertEquals(report.path("queries").get(1).path("hits").get(0).path("relevance").asInt(), 3);
      assertEquals(report.path("queries").get(1).path("missed").size(), 0);
      assertFalse(report.path("probes").get(0).path("passed").asBoolean());
      assertTrue(
          report
              .path("probes")
              .get(0)
              .path("message")
              .asText()
              .contains("urn:li:dataset:expected"));
      assertEquals(
          report.path("probes").get(0).path("hits").get(0).path("urn").asText(),
          "urn:li:dataset:other");
      assertEquals(report.path("probes").get(0).path("hits").get(0).path("score").asDouble(), 1.0);
      ObjectNode passing = new ObjectMapper().createObjectNode();
      passing.put("id", "passing");
      passing.put("query", "q");
      passing.put("exactTopHit", "urn:li:dataset:expected");
      RankedSearchResult found =
          new RankedSearchResult(
              "passing", "keyword", "q", List.of(card("urn:li:dataset:expected")));
      assertEquals(
          SearchQualityReport.probe(passing, found).hits().get(0).urn(), "urn:li:dataset:expected");
      Path profiled =
          SearchQualityReport.write(
              "opensearch", "cloud", "benchmarkNdcgAt10", 0.31, List.of(perfect), List.of());
      assertEquals(profiled.getFileName().toString(), "opensearch-cloud-benchmarkNdcgAt10.json");
      assertEquals(
          new ObjectMapper().readTree(profiled.toFile()).path("profile").asText(), "cloud");
    } finally {
      if (previous == null) {
        System.clearProperty(SearchQualityReport.DIRECTORY_PROPERTY);
      } else {
        System.setProperty(SearchQualityReport.DIRECTORY_PROPERTY, previous);
      }
    }
  }

  @Test
  public void testPackManifestReadsEveryShard() throws Exception {
    assertEquals(SearchQualityPack.shardPaths().size(), 6);
    assertEquals(SearchQualityPack.readShards().size(), 6);
    assertTrue(SearchQualityPack.judgments().path("judgments").size() > 200);
  }

  private static RankedSearchHit card(String urn) {
    return card(urn, 1.0);
  }

  private static RankedSearchHit card(String urn, double score) {
    return new RankedSearchHit(urn, "dataset", "name", "description", score, List.of());
  }

  private static RankedSearchResult hits(String... urns) {
    List<RankedSearchHit> cards = new ArrayList<>();
    for (String urn : urns) {
      cards.add(card(urn));
    }
    return new RankedSearchResult("probe", "keyword", "pet profile", cards);
  }

  private static ObjectNode containsInTop(int window, String... values) {
    ObjectNode judgment = new ObjectMapper().createObjectNode();
    judgment.put("id", "ranking-pet-profile");
    ObjectNode contained = judgment.putObject("containsInTop");
    contained.put("n", window);
    ArrayNode array = contained.putArray("values");
    for (String value : values) {
      array.add(value);
    }
    return judgment;
  }

  private static ObjectNode judgment(
      String id, String query, String group, Map<String, Integer> grades) {
    ObjectNode node = new ObjectMapper().createObjectNode();
    node.put("id", id);
    node.put("query", query);
    node.put("group", group);
    ArrayNode array = node.putArray("grades");
    grades.forEach(
        (urn, relevance) -> {
          ObjectNode grade = array.addObject();
          grade.put("urn", urn);
          grade.put("relevance", relevance);
        });
    return node;
  }
}
