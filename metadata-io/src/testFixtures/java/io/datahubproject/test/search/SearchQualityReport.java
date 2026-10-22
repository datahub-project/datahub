package io.datahubproject.test.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Machine-readable per-query scoring report for one search-quality gate. Each engine writes its own
 * file before the floor assertion. CI uploads the directory when that job fails.
 */
public final class SearchQualityReport {
  public static final String DIRECTORY_PROPERTY = "search.quality.report.dir";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int CUTOFF = 10;

  private SearchQualityReport() {}

  @Nonnull
  public static QueryScore query(@Nonnull JsonNode judgment, @Nonnull RankedSearchResult result) {
    Map<String, Integer> grades = grades(judgment);
    List<String> ranked =
        result.getHits().stream().limit(CUTOFF).map(RankedSearchHit::getUrn).toList();
    List<HitScore> hits = new ArrayList<>();
    for (int i = 0; i < ranked.size(); i++) {
      RankedSearchHit hit = result.getHits().get(i);
      hits.add(
          new HitScore(
              i + 1,
              hit.getUrn(),
              hit.getName(),
              hit.getScore(),
              grades.containsKey(hit.getUrn()) ? grades.get(hit.getUrn()) : null));
    }
    Set<String> seen = new HashSet<>(ranked);
    List<MissedGrade> missed = new ArrayList<>();
    for (Map.Entry<String, Integer> grade : grades.entrySet()) {
      if (grade.getValue() > 0 && !seen.contains(grade.getKey())) {
        missed.add(new MissedGrade(grade.getKey(), grade.getValue()));
      }
    }
    missed.sort(
        Comparator.comparingInt(MissedGrade::relevance).reversed().thenComparing(MissedGrade::urn));
    return new QueryScore(
        judgment.path("id").asText(),
        judgment.path("query").asText(),
        judgment.path("group").asText(),
        QrelScorer.ndcgAt10(ranked, grades),
        QrelScorer.reciprocalRank(ranked, grades),
        QrelScorer.precisionAt(ranked, grades, CUTOFF),
        hits,
        missed);
  }

  @Nonnull
  public static ProbeScore probe(@Nonnull JsonNode judgment, @Nonnull RankedSearchResult result) {
    try {
      TopHitScorer.assertProbe(judgment, result);
      return new ProbeScore(
          judgment.path("id").asText(), judgment.path("query").asText(), true, null);
    } catch (AssertionError failure) {
      String message = failure.getMessage() == null ? "" : failure.getMessage();
      return new ProbeScore(
          judgment.path("id").asText(), judgment.path("query").asText(), false, message);
    }
  }

  /**
   * Writes {@code <engine>-<metric>.json} before the floor assertion. Queries are stored worst nDCG
   * first. {@code passed} is the floor check plus every probe.
   */
  @Nonnull
  public static Path write(
      @Nonnull String engine,
      @Nonnull String metric,
      double floor,
      @Nonnull List<QueryScore> queries,
      @Nonnull List<ProbeScore> probes)
      throws IOException {
    List<QueryScore> ordered = new ArrayList<>(queries);
    ordered.sort(Comparator.comparingDouble(QueryScore::ndcgAt10).thenComparing(QueryScore::id));
    double mean = QrelScorer.mean(ordered.stream().map(QueryScore::ndcgAt10).toList());
    boolean passed = mean + 1e-9 >= floor && probes.stream().allMatch(ProbeScore::passed);
    ObjectNode root = MAPPER.createObjectNode();
    root.put("engine", engine);
    root.put("metric", metric);
    root.put("meanNdcgAt10", mean);
    root.put("floor", floor);
    root.put("passed", passed);
    root.put("queryCount", ordered.size());
    ArrayNode queryNodes = root.putArray("queries");
    for (QueryScore score : ordered) {
      queryNodes.add(score.toJson());
    }
    ArrayNode probeNodes = root.putArray("probes");
    for (ProbeScore probe : probes) {
      probeNodes.add(probe.toJson());
    }
    Path directory = directory();
    Files.createDirectories(directory);
    Path path = directory.resolve(engine + "-" + metric + ".json");
    Files.writeString(path, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root));
    return path.toAbsolutePath();
  }

  @Nonnull
  private static Path directory() {
    String override = System.getProperty(DIRECTORY_PROPERTY);
    if (override != null && !override.isBlank()) {
      return Path.of(override);
    }
    return Path.of("build/reports/search-quality");
  }

  @Nonnull
  private static Map<String, Integer> grades(JsonNode judgment) {
    Map<String, Integer> grades = new LinkedHashMap<>();
    for (JsonNode grade : judgment.path("grades")) {
      grades.put(grade.path("urn").asText(), grade.path("relevance").asInt());
    }
    return grades;
  }

  public static final class QueryScore {
    private final String id;
    private final String query;
    private final String group;
    private final double ndcgAt10;
    private final double reciprocalRank;
    private final double precisionAt10;
    private final List<HitScore> hits;
    private final List<MissedGrade> missed;

    private QueryScore(
        String id,
        String query,
        String group,
        double ndcgAt10,
        double reciprocalRank,
        double precisionAt10,
        List<HitScore> hits,
        List<MissedGrade> missed) {
      this.id = id;
      this.query = query;
      this.group = group;
      this.ndcgAt10 = ndcgAt10;
      this.reciprocalRank = reciprocalRank;
      this.precisionAt10 = precisionAt10;
      this.hits = List.copyOf(hits);
      this.missed = List.copyOf(missed);
    }

    @Nonnull
    public String id() {
      return id;
    }

    public double ndcgAt10() {
      return ndcgAt10;
    }

    @Nonnull
    private ObjectNode toJson() {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("id", id);
      node.put("query", query);
      node.put("group", group);
      node.put("ndcgAt10", ndcgAt10);
      node.put("reciprocalRank", reciprocalRank);
      node.put("precisionAt10", precisionAt10);
      ArrayNode hitNodes = node.putArray("hits");
      for (HitScore hit : hits) {
        hitNodes.add(hit.toJson());
      }
      ArrayNode missedNodes = node.putArray("missed");
      for (MissedGrade grade : missed) {
        missedNodes.add(grade.toJson());
      }
      return node;
    }
  }

  public static final class ProbeScore {
    private final String id;
    private final String query;
    private final boolean passed;
    @Nullable private final String message;

    private ProbeScore(String id, String query, boolean passed, @Nullable String message) {
      this.id = id;
      this.query = query;
      this.passed = passed;
      this.message = message;
    }

    public boolean passed() {
      return passed;
    }

    @Nullable
    public String message() {
      return message;
    }

    @Nonnull
    private ObjectNode toJson() {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("id", id);
      node.put("query", query);
      node.put("passed", passed);
      if (message == null) {
        node.putNull("message");
      } else {
        node.put("message", message);
      }
      return node;
    }
  }

  private static final class HitScore {
    private final int rank;
    private final String urn;
    private final String name;
    private final double score;
    @Nullable private final Integer relevance;

    private HitScore(int rank, String urn, String name, double score, @Nullable Integer relevance) {
      this.rank = rank;
      this.urn = urn;
      this.name = name;
      this.score = score;
      this.relevance = relevance;
    }

    @Nonnull
    private ObjectNode toJson() {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("rank", rank);
      node.put("urn", urn);
      node.put("name", name);
      node.put("score", score);
      if (relevance == null) {
        node.putNull("relevance");
      } else {
        node.put("relevance", relevance);
      }
      return node;
    }
  }

  private static final class MissedGrade {
    private final String urn;
    private final int relevance;

    private MissedGrade(String urn, int relevance) {
      this.urn = urn;
      this.relevance = relevance;
    }

    @Nonnull
    private String urn() {
      return urn;
    }

    private int relevance() {
      return relevance;
    }

    @Nonnull
    private ObjectNode toJson() {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("urn", urn);
      node.put("relevance", relevance);
      return node;
    }
  }
}
