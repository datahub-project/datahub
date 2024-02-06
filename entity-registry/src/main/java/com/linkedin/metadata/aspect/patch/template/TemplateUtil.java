package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.Patch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.util.Pair;
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

  public static List<Pair<PatchOperationType, String>> getPaths(Patch jsonPatch) {
    JsonNode patchNode = OBJECT_MAPPER.valueToTree(jsonPatch);
    List<Pair<PatchOperationType, String>> paths = new ArrayList<>();
    patchNode
        .elements()
        .forEachRemaining(
            node ->
                paths.add(
                    Pair.of(
                        PatchOperationType.valueOf(node.get("op").asText().toUpperCase()),
                        node.get("path").asText())));
    return paths;
  }

  public static void validatePatch(Patch jsonPatch) {
    // ensure supported patch operations
    JsonNode patchNode = OBJECT_MAPPER.valueToTree(jsonPatch);
    patchNode
        .elements()
        .forEachRemaining(
            node -> {
              try {
                PatchOperationType.valueOf(node.get("op").asText().toUpperCase());
              } catch (Exception e) {
                throw new RuntimeException(
                    String.format(
                        "Unsupported PATCH operation: `%s` Operation `%s`",
                        node.get("op").asText(), node),
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
  public static JsonNode populateTopLevelKeys(JsonNode transformedNode, Patch jsonPatch) {
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
        if (parent.get(keys[i]) == null) {
          ((ObjectNode) parent).set(keys[i], instance.objectNode());
        }
        parent = parent.get(keys[i]);
      }
    }

    return transformedNodeClone;
  }
}
