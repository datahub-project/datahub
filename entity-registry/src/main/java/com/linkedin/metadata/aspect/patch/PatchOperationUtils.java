package com.linkedin.metadata.aspect.patch;

import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.util.Pair;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Helpers for validators that check the values a JSON-patch write introduces at the request stage
 * (proposed hook), without loading the stored aspect. Only add/replace operations carry values;
 * interpretation is best-effort — a shape the caller cannot parse should be skipped rather than
 * rejected, since schema validation still applies when the patch is merged.
 */
@Slf4j
public final class PatchOperationUtils {
  private PatchOperationUtils() {}

  /**
   * The (path, value) of each add/replace operation in the item's patch. A {@link PatchMCP} exposes
   * the parsed patch directly; under alternate MCP validation (the quickstart/docker default,
   * {@code ALTERNATE_MCP_VALIDATION=true}) a patch arrives as a raw proposal item whose aspect
   * payload is the serialized patch — either a bare json-patch ops array or a {@code
   * GenericJsonPatch} object wrapping one under {@code patch}.
   */
  public static Stream<Pair<String, JsonValue>> addAndReplaceValues(@Nonnull MCPItem item) {
    return patchOperations(item).stream()
        .filter(op -> op.getValueType() == JsonValue.ValueType.OBJECT)
        .map(JsonValue::asJsonObject)
        .filter(
            op -> {
              String type = op.getString("op", "");
              return ("add".equals(type) || "replace".equals(type)) && op.get("value") != null;
            })
        .map(op -> Pair.of(op.getString("path", ""), op.get("value")));
  }

  /**
   * Rebuilds a partial aspect from an operation by nesting its value under the object fields of its
   * path — e.g. a value at {@code /resources/filter} becomes {@code
   * {"resources":{"filter":<value>}}}, and a root-path value is returned as-is. Empty for paths
   * that address array elements (numeric segments), which cannot be reconstructed as objects.
   */
  public static Optional<JsonValue> nestValueAtObjectPath(
      @Nonnull String path, @Nonnull JsonValue value) {
    List<String> segments =
        Arrays.stream(path.split("/")).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    if (segments.stream().anyMatch(s -> s.chars().allMatch(Character::isDigit))) {
      return Optional.empty();
    }
    JsonValue current = value;
    for (int i = segments.size() - 1; i >= 0; i--) {
      current = Json.createObjectBuilder().add(segments.get(i), current).build();
    }
    return Optional.of(current);
  }

  @Nonnull
  private static JsonArray patchOperations(@Nonnull MCPItem item) {
    if (item instanceof PatchMCP) {
      return ((PatchMCP) item).getPatch().toJsonArray();
    }
    if (item.getMetadataChangeProposal() == null || !item.getMetadataChangeProposal().hasAspect()) {
      return JsonValue.EMPTY_JSON_ARRAY;
    }
    try (JsonReader reader =
        Json.createReader(
            new StringReader(
                item.getMetadataChangeProposal()
                    .getAspect()
                    .getValue()
                    .asString(StandardCharsets.UTF_8)))) {
      JsonValue parsed = reader.readValue();
      if (parsed.getValueType() == JsonValue.ValueType.ARRAY) {
        return parsed.asJsonArray();
      }
      if (parsed.getValueType() == JsonValue.ValueType.OBJECT) {
        JsonValue patch = parsed.asJsonObject().get(GenericJsonPatch.PATCH_FIELD);
        if (patch != null && patch.getValueType() == JsonValue.ValueType.ARRAY) {
          return patch.asJsonArray();
        }
      }
    } catch (RuntimeException e) {
      log.warn(
          "Skipping unparseable patch payload on {} {}: {}",
          item.getUrn(),
          item.getAspectName(),
          e.toString());
    }
    return JsonValue.EMPTY_JSON_ARRAY;
  }
}
