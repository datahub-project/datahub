package io.datahubproject.test.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Resolves the in-repo search-quality datapack. CI does not download it. */
public final class SearchQualityPack {
  public static final String DIRECTORY_ENV = "SEARCH_QUALITY_DATAPACK_DIR";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final List<String> SURFACES = List.of("keyword", "semantic", "hybrid", "lineage");
  private static final List<String> GROUPS =
      List.of(
          "benchmark",
          "longtail-ecommerce",
          "entity-boost",
          "entity-search",
          "keyword-probe",
          "semantic-probe",
          "structured-property",
          "sort",
          "field-probe",
          "autocomplete");

  private SearchQualityPack() {}

  @Nonnull
  public static Path directory() {
    String override = System.getenv(DIRECTORY_ENV);
    if (override != null && !override.isBlank()) {
      return Path.of(override);
    }
    return Path.of("src/test/resources/datapacks/search-quality");
  }

  @Nonnull
  public static JsonNode index() throws IOException {
    return MAPPER.readTree(directory().resolve("index.json").toFile());
  }

  @Nonnull
  public static List<Path> shardPaths() throws IOException {
    return load("").shardPaths();
  }

  @Nonnull
  public static List<JsonNode> readShards() throws IOException {
    return load("").shards();
  }

  @Nonnull
  public static JsonNode judgments() throws IOException {
    return load("").judgments();
  }

  /**
   * Default profile reads {@code index.json} and {@code judgments.json}. A named profile also reads
   * {@code index.{profile}.json} and {@code judgments.{profile}.json}.
   */
  @Nonnull
  public static Loaded load(@Nullable String profile) throws IOException {
    return load(directory(), profile);
  }

  /** Loads a pack from an explicit directory for fixture validation tests. */
  @Nonnull
  public static Loaded load(@Nonnull Path root, @Nullable String profile) throws IOException {
    List<Path> paths = files(root.resolve("index.json"));
    ObjectNode judgments = (ObjectNode) MAPPER.readTree(root.resolve("judgments.json").toFile());
    List<String> extraGroups = new ArrayList<>();
    Map<String, Divergence> divergences = Map.of();
    if (profile != null && !profile.isBlank()) {
      Path extraIndex = root.resolve("index." + profile + ".json");
      Path extraJudgments = root.resolve("judgments." + profile + ".json");
      if (Files.isRegularFile(extraIndex)) {
        paths.addAll(files(extraIndex));
      }
      if (Files.isRegularFile(extraJudgments)) {
        JsonNode extra = MAPPER.readTree(extraJudgments.toFile());
        extra.path("groups").forEach(node -> extraGroups.add(node.asText()));
        if (extra.has("floors")) {
          throw new IllegalStateException(
              "Profile " + profile + " must use floorOffsets and cloudFloors, not floors");
        }
        applyFloorOffsets(judgments, extra.path("floorOffsets"), profile);
        applyCloudFloors(judgments, extra.path("cloudFloors"), profile);
        com.fasterxml.jackson.databind.node.ArrayNode base =
            (com.fasterxml.jackson.databind.node.ArrayNode) judgments.withArray("judgments");
        Set<String> ids = new java.util.HashSet<>();
        for (JsonNode judgment : base) {
          ids.add(judgment.path("id").asText());
        }
        for (JsonNode judgment : extra.path("judgments")) {
          String id = judgment.path("id").asText();
          if (!ids.add(id)) {
            throw new IllegalStateException("Profile " + profile + " rewrites judgment " + id);
          }
          base.add(judgment);
        }
        divergences = divergences(extra.path("divergences"), ids, profile);
      }
    }
    List<JsonNode> shards = new ArrayList<>();
    for (Path path : paths) {
      if (!Files.isRegularFile(path)) {
        throw new IllegalStateException("Missing datapack shard " + path);
      }
      JsonNode shard = MAPPER.readTree(path.toFile());
      if (!shard.isArray() || shard.isEmpty()) {
        throw new IllegalStateException("Shard " + path + " is not a non-empty MCP array");
      }
      shards.add(shard);
    }
    validate(judgments, urns(shards), extraGroups);
    return new Loaded(paths, shards, judgments, divergences);
  }

  private static void applyFloorOffsets(
      @Nonnull ObjectNode judgments, @Nonnull JsonNode offsets, @Nonnull String profile) {
    if (offsets.isMissingNode() || offsets.isNull()) {
      return;
    }
    if (!offsets.isObject()) {
      throw new IllegalStateException("Profile " + profile + " floorOffsets must be an object");
    }
    ObjectNode floors = (ObjectNode) judgments.with("floors");
    offsets
        .fields()
        .forEachRemaining(
            engine -> {
              if (!engine.getValue().isObject()) {
                throw new IllegalStateException(
                    "Profile "
                        + profile
                        + " floorOffsets."
                        + engine.getKey()
                        + " must be an object");
              }
              ObjectNode base = (ObjectNode) floors.with(engine.getKey());
              engine
                  .getValue()
                  .fields()
                  .forEachRemaining(
                      metric -> {
                        JsonNode offset = metric.getValue();
                        if (!offset.isObject()
                            || !offset.path("value").isNumber()
                            || !Double.isFinite(offset.path("value").asDouble())
                            || !offset.path("reason").isTextual()
                            || offset.path("reason").asText().isBlank()) {
                          throw new IllegalStateException(
                              "Profile "
                                  + profile
                                  + " floorOffsets."
                                  + engine.getKey()
                                  + "."
                                  + metric.getKey()
                                  + " requires finite value and reason");
                        }
                        if (!base.has(metric.getKey())) {
                          throw new IllegalStateException(
                              "Profile "
                                  + profile
                                  + " offsets unknown core floor "
                                  + engine.getKey()
                                  + "."
                                  + metric.getKey());
                        }
                        base.put(
                            metric.getKey(),
                            base.path(metric.getKey()).asDouble()
                                + offset.path("value").asDouble());
                      });
            });
  }

  private static void applyCloudFloors(
      @Nonnull ObjectNode judgments, @Nonnull JsonNode cloudFloors, @Nonnull String profile) {
    if (cloudFloors.isMissingNode() || cloudFloors.isNull()) {
      return;
    }
    if (!cloudFloors.isObject()) {
      throw new IllegalStateException("Profile " + profile + " cloudFloors must be an object");
    }
    ObjectNode floors = (ObjectNode) judgments.with("floors");
    cloudFloors
        .fields()
        .forEachRemaining(
            engine -> {
              if (!engine.getValue().isObject()) {
                throw new IllegalStateException(
                    "Profile "
                        + profile
                        + " cloudFloors."
                        + engine.getKey()
                        + " must be an object");
              }
              ObjectNode target = (ObjectNode) floors.with(engine.getKey());
              engine
                  .getValue()
                  .fields()
                  .forEachRemaining(
                      metric -> {
                        if (!metric.getValue().isNumber()
                            || !Double.isFinite(metric.getValue().asDouble())) {
                          throw new IllegalStateException(
                              "Profile "
                                  + profile
                                  + " cloudFloors."
                                  + engine.getKey()
                                  + "."
                                  + metric.getKey()
                                  + " must be finite");
                        }
                        if (target.has(metric.getKey())) {
                          throw new IllegalStateException(
                              "Profile "
                                  + profile
                                  + " cloudFloors may not replace core floor "
                                  + engine.getKey()
                                  + "."
                                  + metric.getKey());
                        }
                        target.set(metric.getKey(), metric.getValue());
                      });
            });
  }

  @Nonnull
  private static Map<String, Divergence> divergences(
      @Nonnull JsonNode source, @Nonnull Set<String> judgmentIds, @Nonnull String profile) {
    if (source.isMissingNode() || source.isNull()) {
      return Map.of();
    }
    if (!source.isArray()) {
      throw new IllegalStateException("Profile " + profile + " divergences must be an array");
    }
    Map<String, Divergence> result = new LinkedHashMap<>();
    for (JsonNode node : source) {
      String id = node.path("id").asText();
      String reason = node.path("reason").asText();
      if (id.isBlank() || !judgmentIds.contains(id)) {
        throw new IllegalStateException(
            "Profile " + profile + " divergence names unknown judgment " + id);
      }
      if (reason.isBlank()) {
        throw new IllegalStateException(
            "Profile " + profile + " divergence " + id + " needs a reason");
      }
      boolean hasDelta = node.has("ndcgDelta");
      boolean hasProbe = node.has("probe");
      if (hasDelta == hasProbe) {
        throw new IllegalStateException(
            "Profile "
                + profile
                + " divergence "
                + id
                + " needs exactly one of ndcgDelta or probe");
      }
      if (result.containsKey(id)) {
        throw new IllegalStateException("Profile " + profile + " has duplicate divergence " + id);
      }
      if (hasDelta) {
        double delta = node.path("ndcgDelta").asDouble(Double.NaN);
        if (!Double.isFinite(delta) || delta >= 0.0) {
          throw new IllegalStateException(
              "Profile "
                  + profile
                  + " divergence "
                  + id
                  + " ndcgDelta must be finite and negative");
        }
        result.put(id, new Divergence(id, reason, delta, null));
      } else {
        JsonNode probe = node.path("probe");
        if (!probe.isObject() || probe.isEmpty()) {
          throw new IllegalStateException(
              "Profile " + profile + " divergence " + id + " probe must be a non-empty object");
        }
        probe
            .fieldNames()
            .forEachRemaining(
                field -> {
                  if (!Set.of("exactTopHit", "orderedContains", "containsInTop", "present")
                      .contains(field)) {
                    throw new IllegalStateException(
                        "Profile "
                            + profile
                            + " divergence "
                            + id
                            + " has unsupported probe field "
                            + field);
                  }
                });
        result.put(id, new Divergence(id, reason, null, probe.deepCopy()));
      }
    }
    return Collections.unmodifiableMap(result);
  }

  @Nonnull
  public static java.util.Set<String> urns(@Nonnull List<JsonNode> shards) {
    java.util.Set<String> urns = new java.util.HashSet<>();
    for (JsonNode shard : shards) {
      for (JsonNode proposal : shard) {
        urns.add(proposal.path("entityUrn").asText());
      }
    }
    return urns;
  }

  public static void validate(@Nonnull JsonNode judgments, @Nonnull java.util.Set<String> urns) {
    validate(judgments, urns, List.of());
  }

  public static void validate(
      @Nonnull JsonNode judgments,
      @Nonnull java.util.Set<String> urns,
      @Nonnull List<String> extraGroups) {
    java.util.Set<String> groups = new java.util.HashSet<>(GROUPS);
    groups.addAll(extraGroups);
    for (JsonNode judgment : judgments.path("judgments")) {
      String id = judgment.path("id").asText();
      String group = judgment.path("group").asText();
      if (!groups.contains(group)) {
        throw new IllegalStateException("Unknown judgment group on " + id);
      }
      for (JsonNode surface : judgment.path("surfaces")) {
        if (!SURFACES.contains(surface.asText())) {
          throw new IllegalStateException("Unknown surface on " + id);
        }
        if ("benchmark".equals(group)
            && ("semantic".equals(surface.asText()) || "hybrid".equals(surface.asText()))) {
          throw new IllegalStateException("Benchmark query " + id + " is not keyword-only");
        }
      }
      for (JsonNode grade : judgment.path("grades")) {
        if (!grade.path("relevance").isIntegralNumber()) {
          throw new IllegalStateException("Malformed grade on " + id);
        }
        if (!urns.contains(grade.path("urn").asText())) {
          throw new IllegalStateException("Judgment " + id + " references an unloaded URN");
        }
      }
      for (JsonNode urn : judgment.path("expectedOrder")) {
        if (!urns.contains(urn.asText())) {
          throw new IllegalStateException("Judgment " + id + " expects an unloaded URN");
        }
      }
    }
  }

  @Nonnull
  private static List<Path> files(@Nonnull Path index) throws IOException {
    List<Path> paths = new ArrayList<>();
    JsonNode root = MAPPER.readTree(index.toFile());
    for (JsonNode file : root.path("files")) {
      String name = file.isTextual() ? file.asText() : file.path("path").asText();
      paths.add(index.getParent().resolve(name));
    }
    return paths;
  }

  public static final class Loaded {
    private final List<Path> shardPaths;
    private final List<JsonNode> shards;
    private final JsonNode judgments;
    private final Map<String, Divergence> divergences;

    private Loaded(
        @Nonnull List<Path> shardPaths,
        @Nonnull List<JsonNode> shards,
        @Nonnull JsonNode judgments,
        @Nonnull Map<String, Divergence> divergences) {
      this.shardPaths = shardPaths;
      this.shards = shards;
      this.judgments = judgments;
      this.divergences = divergences;
    }

    @Nonnull
    public List<Path> shardPaths() {
      return shardPaths;
    }

    @Nonnull
    public List<JsonNode> shards() {
      return shards;
    }

    public int shardCount() {
      return shards.size();
    }

    @Nonnull
    public JsonNode judgments() {
      return judgments;
    }

    @Nonnull
    public Map<String, Divergence> divergences() {
      return divergences;
    }
  }

  public record Divergence(
      @Nonnull String id,
      @Nonnull String reason,
      @Nullable Double ndcgDelta,
      @Nullable JsonNode probe) {}
}
