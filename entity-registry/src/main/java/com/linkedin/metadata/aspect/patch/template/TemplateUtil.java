package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.util.Pair;
import jakarta.json.JsonPatch;
import jakarta.json.JsonValue;
import java.util.ArrayList;
import java.util.List;

public class TemplateUtil {

  private TemplateUtil() {}

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  public static List<Pair<PatchOperationType, String>> getPaths(JsonPatch jsonPatch) {
    List<Pair<PatchOperationType, String>> paths = new ArrayList<>();
    jsonPatch.toJsonArray().stream()
        .map(JsonValue::asJsonObject)
        .forEach(
            node ->
                paths.add(
                    Pair.of(
                        PatchOperationType.valueOf(node.getString("op").toUpperCase()),
                        node.getString("path"))));
    return paths;
  }

  public static void validatePatch(JsonPatch jsonPatch) {
    // ensure supported patch operations
    jsonPatch.toJsonArray().stream()
        .map(JsonValue::asJsonObject)
        .forEach(
            jsonObject -> {
              try {
                PatchOperationType.valueOf(jsonObject.getString("op").toUpperCase());
              } catch (Exception e) {
                throw new RuntimeException(
                    String.format(
                        "Unsupported PATCH operation: `%s` Operation `%s`",
                        jsonObject.getString("op"), jsonObject),
                    e);
              }
            });
  }

  /**
   * Necessary step for templates with compound keys due to JsonPatch not allowing non-existent
   * paths to be specified
   *
   * @param transformedNode transformed node to have keys populated
   * @return transformed node that has top level keys populated
   */
  public static JsonNode populateTopLevelKeys(JsonNode transformedNode, JsonPatch jsonPatch) {
    JsonNode transformedNodeClone = transformedNode.deepCopy();
    List<Pair<PatchOperationType, String>> paths = getPaths(jsonPatch);
    for (Pair<PatchOperationType, String> operationPath : paths) {
      String[] keys = operationPath.getSecond().split("/");
      JsonNode parent = transformedNodeClone;

      // if not remove, skip last key as we only need to populate top level
      int endIdx =
          PatchOperationType.REMOVE.equals(operationPath.getFirst())
              ? keys.length
              : keys.length - 1;
      // Skip first as it will always be blank due to path starting with /
      for (int i = 1; i < endIdx; i++) {
        String decodedKey = decodeValue(keys[i]);
        if (parent.get(decodedKey) == null) {
          ((ObjectNode) parent).set(decodedKey, instance.objectNode());
        }
        parent = parent.get(decodedKey);
      }
    }

    return transformedNodeClone;
  }

  /** Simply decode a JSON-patch encoded value * */
  private static String decodeValue(String value) {
    return value.replace("~1", "/").replace("~0", "~");
  }
}
