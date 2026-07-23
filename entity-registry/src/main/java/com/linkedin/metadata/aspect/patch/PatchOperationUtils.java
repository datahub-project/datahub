package com.linkedin.metadata.aspect.patch;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_NAME_LENGTH;
import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_NAME_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared helpers for JSON-patch MCP payloads: write-path construction ({@link #convertToJsonPatch})
 * and request-stage validators under alternate MCP validation ({@link #resolveGenericJsonPatch},
 * {@link #addAndReplaceValues}).
 */
@Slf4j
public final class PatchOperationUtils {
  /** Jackson mapper for patch payloads (ingestion stream-constraint env overrides). */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    int maxNameLength =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_NAME_LENGTH, MAX_JACKSON_NAME_LENGTH));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder()
                .maxStringLength(maxSize)
                .maxNameLength(maxNameLength)
                .build());
  }

  private PatchOperationUtils() {}

  /**
   * Parse an MCP aspect payload into a {@link JsonPatch} and, when possible, a {@link
   * GenericJsonPatch}. Accepts either a {@code GenericJsonPatch} object ({@code patch} array plus
   * optional metadata) or a bare json-patch ops array. Throws on invalid input (write-path).
   */
  @Nonnull
  public static Pair<JsonPatch, Optional<GenericJsonPatch>> convertToJsonPatch(
      @Nonnull MetadataChangeProposal mcp) {
    if (mcp.getAspect() == null || mcp.getAspect().getValue() == null) {
      throw new IllegalArgumentException("PATCH MCP is missing aspect payload");
    }
    return convertToJsonPatch(mcp.getAspect().getValue().asString(StandardCharsets.UTF_8));
  }

  @Nonnull
  public static Pair<JsonPatch, Optional<GenericJsonPatch>> convertToJsonPatch(
      @Nonnull String jsonString) {
    try {
      JsonStructure jsonStructure;
      try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
        jsonStructure = reader.read();
      }

      if (jsonStructure.getValueType() == JsonValue.ValueType.OBJECT) {
        JsonObject jsonObject = (JsonObject) jsonStructure;
        if (jsonObject.containsKey(GenericJsonPatch.PATCH_FIELD)) {
          JsonPatch jsonPatch =
              Json.createPatch(jsonObject.getJsonArray(GenericJsonPatch.PATCH_FIELD));
          try {
            GenericJsonPatch genericJsonPatch =
                OBJECT_MAPPER.readValue(jsonString, GenericJsonPatch.class);
            return Pair.of(jsonPatch, Optional.of(genericJsonPatch));
          } catch (Exception e) {
            // Ops such as move/copy include "from", which PatchOp cannot model. Keep JsonPatch.
            return Pair.of(jsonPatch, Optional.empty());
          }
        }
      }

      if (jsonStructure.getValueType() == JsonValue.ValueType.ARRAY) {
        JsonPatch jsonPatch = Json.createPatch(jsonStructure.asJsonArray());
        // Bare arrays may include RFC-6902 move/copy (with "from") that PatchOp cannot model.
        // Keep JsonPatch applyable and omit GenericJsonPatch rather than failing the write path.
        try {
          List<GenericJsonPatch.PatchOp> ops =
              OBJECT_MAPPER.readValue(
                  jsonString, new TypeReference<List<GenericJsonPatch.PatchOp>>() {});
          return Pair.of(jsonPatch, Optional.of(GenericJsonPatch.builder().patch(ops).build()));
        } catch (Exception e) {
          return Pair.of(jsonPatch, Optional.empty());
        }
      }

      // Object without a "patch" key — preserve prior write-path behavior (treat as array).
      JsonPatch jsonPatch =
          Json.createPatch(Json.createReader(new StringReader(jsonString)).readArray());
      return Pair.of(jsonPatch, Optional.empty());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid JSON Patch: " + jsonString, e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Best-effort {@link GenericJsonPatch} for validators: reuse a parsed {@link PatchMCP} value when
   * present, otherwise parse the MCP aspect payload via {@link #convertToJsonPatch}. Returns null
   * when the item has no payload or the payload cannot be parsed.
   */
  @Nullable
  public static GenericJsonPatch resolveGenericJsonPatch(@Nonnull MCPItem item) {
    if (item instanceof PatchMCP) {
      GenericJsonPatch existing = ((PatchMCP) item).getGenericJsonPatch();
      if (existing != null) {
        return existing;
      }
    }
    if (item.getMetadataChangeProposal() == null || !item.getMetadataChangeProposal().hasAspect()) {
      return null;
    }
    try {
      return convertToJsonPatch(item.getMetadataChangeProposal()).getSecond().orElse(null);
    } catch (RuntimeException e) {
      log.warn(
          "Skipping unparseable PATCH payload on {} {}: {}",
          item.getUrn(),
          item.getAspectName(),
          e.toString());
      return null;
    }
  }

  /**
   * Best-effort {@link JsonPatch} for applying the full patch (including move/copy). Prefers an
   * already-parsed {@link PatchMCP}; otherwise parses via {@link #convertToJsonPatch}.
   */
  @Nullable
  public static JsonPatch resolveJsonPatch(@Nonnull MCPItem item) {
    if (item instanceof PatchMCP) {
      return ((PatchMCP) item).getPatch();
    }
    if (item.getMetadataChangeProposal() == null || !item.getMetadataChangeProposal().hasAspect()) {
      return null;
    }
    try {
      return convertToJsonPatch(item.getMetadataChangeProposal()).getFirst();
    } catch (RuntimeException e) {
      log.warn(
          "Skipping unparseable PATCH payload on {} {}: {}",
          item.getUrn(),
          item.getAspectName(),
          e.toString());
      return null;
    }
  }

  /**
   * The (path, value) of each add/replace operation in the item's patch. A {@link PatchMCP} exposes
   * the parsed patch directly; under alternate MCP validation a patch arrives as a raw proposal
   * whose aspect payload is the serialized patch (bare ops array or {@link GenericJsonPatch}).
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
   * {"resources":{"filter":value}}}, and a root-path value is returned as-is. Empty for paths that
   * address array elements (numeric segments), which cannot be reconstructed as objects.
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

  /**
   * Ops array for the item. Prefers an already-parsed {@link PatchMCP}; otherwise parses via {@link
   * #convertToJsonPatch} so ProposedItem and write-path construction share one payload parser.
   */
  @Nonnull
  private static JsonArray patchOperations(@Nonnull MCPItem item) {
    if (item instanceof PatchMCP) {
      return ((PatchMCP) item).getPatch().toJsonArray();
    }
    if (item.getMetadataChangeProposal() == null || !item.getMetadataChangeProposal().hasAspect()) {
      return JsonValue.EMPTY_JSON_ARRAY;
    }
    try {
      return convertToJsonPatch(item.getMetadataChangeProposal()).getFirst().toJsonArray();
    } catch (RuntimeException e) {
      log.warn(
          "Skipping unparseable patch payload on {} {}: {}",
          item.getUrn(),
          item.getAspectName(),
          e.toString());
      return JsonValue.EMPTY_JSON_ARRAY;
    }
  }
}
