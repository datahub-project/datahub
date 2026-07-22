package com.linkedin.metadata.aspect.patch;

import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.util.Pair;
import jakarta.json.Json;
import jakarta.json.JsonValue;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Helpers for validators that check the values a JSON-patch write introduces at the request stage
 * (proposed hook), without loading the stored aspect. Only add/replace operations carry values;
 * interpretation is best-effort — a shape the caller cannot parse should be skipped rather than
 * rejected, since schema validation still applies when the patch is merged.
 */
public final class PatchOperationUtils {
  private PatchOperationUtils() {}

  /** The (path, value) of each add/replace operation in the item's patch. */
  public static Stream<Pair<String, JsonValue>> addAndReplaceValues(@Nonnull PatchMCP item) {
    return item.getPatch().toJsonArray().stream()
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
}
