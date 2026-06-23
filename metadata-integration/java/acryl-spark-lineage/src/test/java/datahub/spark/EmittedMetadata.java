package datahub.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Parsed view of the Spark file-emitter output (a JSON array of MCPs) with query helpers, so smoke
 * tests can assert on lineage edges and aspect content instead of matching raw substrings.
 *
 * <p>The emitter double-serializes: each MCP's {@code aspect.value} is itself a JSON string, and
 * dataset URNs are referenced inside the {@code dataJobInputOutput} edges rather than emitted as
 * standalone dataset entities. {@link #aspect} de-stringifies the value; {@link #inputDatasetUrns}
 * / {@link #outputDatasetUrns} pull the lineage edges.
 */
final class EmittedMetadata {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  final String raw;
  final List<JsonNode> mcps;

  private EmittedMetadata(String raw, List<JsonNode> mcps) {
    this.raw = raw;
    this.mcps = mcps;
  }

  static EmittedMetadata parse(String raw) {
    List<JsonNode> mcps = new ArrayList<>();
    try {
      if (raw != null && !raw.trim().isEmpty()) {
        JsonNode arr = MAPPER.readTree(raw);
        if (arr.isArray()) {
          arr.forEach(mcps::add);
        }
      }
    } catch (Exception e) {
      // Fall back to raw-only assertions if the array isn't well-formed.
    }
    return new EmittedMetadata(raw, mcps);
  }

  boolean contains(String s) {
    return raw != null && raw.contains(s);
  }

  boolean hasAspect(String aspectName) {
    return mcps.stream().anyMatch(m -> aspectName.equals(text(m, "aspectName")));
  }

  /**
   * Whether any MCP was emitted for the given entityType (e.g. {@code dataset}, {@code dataJob}).
   */
  boolean hasEntity(String entityType) {
    return mcps.stream().anyMatch(m -> entityType.equals(text(m, "entityType")));
  }

  /** The parsed (de-stringified) value of the first MCP carrying {@code aspectName}. */
  Optional<JsonNode> aspect(String aspectName) {
    return mcps.stream()
        .filter(m -> aspectName.equals(text(m, "aspectName")))
        .map(EmittedMetadata::aspectValue)
        .filter(Objects::nonNull)
        .findFirst();
  }

  /** Same as {@link #aspect(String)} but restricted to an entityType (e.g. {@code "dataJob"}). */
  Optional<JsonNode> aspect(String entityType, String aspectName) {
    return mcps.stream()
        .filter(
            m ->
                entityType.equals(text(m, "entityType"))
                    && aspectName.equals(text(m, "aspectName")))
        .map(EmittedMetadata::aspectValue)
        .filter(Objects::nonNull)
        .findFirst();
  }

  Set<String> inputDatasetUrns() {
    return edgeUrns("inputDatasetEdges");
  }

  Set<String> outputDatasetUrns() {
    return edgeUrns("outputDatasetEdges");
  }

  /** All dataset URNs referenced by the job's input + output lineage edges. */
  Set<String> datasetEdgeUrns() {
    Set<String> all = new LinkedHashSet<>(inputDatasetUrns());
    all.addAll(outputDatasetUrns());
    return all;
  }

  /** Lineage-edge dataset URNs on the given DataHub platform (e.g. {@code glue}, {@code file}). */
  Set<String> datasetUrnsOnPlatform(String platform) {
    String token = "urn:li:dataPlatform:" + platform + ",";
    Set<String> out = new LinkedHashSet<>();
    for (String urn : datasetEdgeUrns()) {
      if (urn.contains(token)) {
        out.add(urn);
      }
    }
    return out;
  }

  private Set<String> edgeUrns(String edgeField) {
    Set<String> urns = new LinkedHashSet<>();
    aspect("dataJobInputOutput")
        .map(io -> io.get(edgeField))
        .ifPresent(
            edges -> {
              if (edges.isArray()) {
                edges.forEach(
                    e -> {
                      JsonNode dest = e.get("destinationUrn");
                      if (dest != null) {
                        urns.add(dest.asText());
                      }
                    });
              }
            });
    return urns;
  }

  private static JsonNode aspectValue(JsonNode mcp) {
    JsonNode aspect = mcp.get("aspect");
    JsonNode value = aspect == null ? null : aspect.get("value");
    if (value == null) {
      return null;
    }
    try {
      // The spark file emitter double-serializes: aspect.value is a JSON string.
      return MAPPER.readTree(value.asText());
    } catch (Exception e) {
      return null;
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return v == null ? null : v.asText();
  }
}
