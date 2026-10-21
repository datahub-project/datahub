package io.datahubproject.test.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/** Resolves the in-repo search-quality datapack. CI does not download it. */
public final class SearchQualityPack {
  public static final String DIRECTORY_ENV = "SEARCH_QUALITY_DATAPACK_DIR";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final List<String> SURFACES = List.of("keyword", "semantic", "hybrid", "lineage");
  private static final List<String> GROUPS =
      List.of("benchmark", "longtail", "ranking", "catalog-probe", "semantic-probe");

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
    List<Path> paths = new ArrayList<>();
    for (JsonNode file : index().path("files")) {
      String name = file.isTextual() ? file.asText() : file.path("path").asText();
      Path path = directory().resolve(name);
      if (!Files.isRegularFile(path)) {
        throw new IllegalStateException("Missing datapack shard " + path);
      }
      paths.add(path);
    }
    return paths;
  }

  @Nonnull
  public static List<JsonNode> readShards() throws IOException {
    List<JsonNode> shards = new ArrayList<>();
    for (Path path : shardPaths()) {
      JsonNode shard = MAPPER.readTree(path.toFile());
      if (!shard.isArray() || shard.isEmpty()) {
        throw new IllegalStateException("Shard " + path + " is not a non-empty MCP array");
      }
      shards.add(shard);
    }
    return shards;
  }

  @Nonnull
  public static JsonNode judgments() throws IOException {
    JsonNode judgments = MAPPER.readTree(directory().resolve("judgments.json").toFile());
    validate(judgments, urns(readShards()));
    return judgments;
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
    for (JsonNode judgment : judgments.path("judgments")) {
      String id = judgment.path("id").asText();
      String group = judgment.path("group").asText();
      if (!GROUPS.contains(group)) {
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
    }
  }
}
