package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.List;

public interface ArrayMergingTemplate<T extends RecordTemplate> extends Template<T> {

  static final String UNIT_SEPARATOR_DELIMITER = "␟";

  /**
   * Takes an Array field on the {@link RecordTemplate} subtype along with a set of key fields to
   * transform into a map Avoids producing side effects by copying nodes, use resulting node and not
   * the original
   *
   * @param baseNode the base unmodified node
   * @param arrayFieldName name of the array field to be transformed
   * @param keyFields subfields of the array object to be used as keys, empty implies the list is
   *     just strings to be merged
   * @return the modified {@link JsonNode} with array fields transformed to maps
   */
  default JsonNode arrayFieldToMap(
      JsonNode baseNode, String arrayFieldName, List<String> keyFields) {
    JsonNode transformedNode = baseNode.deepCopy();
    JsonNode arrayNode = baseNode.get(arrayFieldName);
    ObjectNode mapNode = instance.objectNode();
    if (arrayNode instanceof ArrayNode) {

      ((ArrayNode) arrayNode)
          .elements()
          .forEachRemaining(
              node -> {
                ObjectNode keyValue = mapNode;
                // Creates nested object of keys with final value being the full value of the node
                JsonNode nodeClone = node.deepCopy();
                if (!keyFields.isEmpty()) {
                  for (String keyField : keyFields) {
                    String key = "";
                    // if the keyField has a unit separator, we are working with a nested key
                    if (keyField.contains(UNIT_SEPARATOR_DELIMITER)) {
                      String[] keyParts = keyField.split(UNIT_SEPARATOR_DELIMITER);
                      JsonNode keyObject = node;

                      // Traverse through all parts of the key path
                      try {
                        for (int i = 0; i < keyParts.length - 1; i++) {
                          keyObject = keyObject.get(keyParts[i]);
                        }

                        // Get the final key value, defaulting to empty string if not found
                        JsonNode finalKeyNode = keyObject.get(keyParts[keyParts.length - 1]);
                        key = finalKeyNode != null ? finalKeyNode.asText() : "";
                      } catch (NullPointerException | IllegalArgumentException e) {
                        // If any part of the path is missing, use an empty string
                        key = "";
                      }
                    } else {
                      // Get the key, defaulting to empty string if not found
                      JsonNode keyNode = node.get(keyField);
                      key = keyNode != null ? keyNode.asText() : "";
                    }
                    keyValue =
                        keyValue.get(key) == null
                            ? (ObjectNode) keyValue.set(key, instance.objectNode()).get(key)
                            : (ObjectNode) keyValue.get(key);
                  }
                } else {
                  // No key fields, assume String array
                  nodeClone =
                      instance
                          .objectNode()
                          .set(node instanceof TextNode ? ((TextNode) node).asText() : "", node);
                }
                keyValue.setAll((ObjectNode) nodeClone);
              });
    }
    return ((ObjectNode) transformedNode).set(arrayFieldName, mapNode);
  }

  /**
   * Takes a transformed map field on the {@link JsonNode} representation along with a set of key
   * fields used to transform into a map and rebases it to the original defined format Avoids
   * producing side effects by copying nodes, use resulting node and not the original
   *
   * @param transformedNode the transformed node
   * @param arrayFieldName name of the array field to be transformed
   * @param keyFields subfields of the array object to be used as keys, empty implies the list is
   *     just strings to be merged
   * @return the modified {@link JsonNode} formatted consistent with the original schema
   */
  default JsonNode transformedMapToArray(
      JsonNode transformedNode, String arrayFieldName, List<String> keyFields) {
    JsonNode fieldNode = transformedNode.get(arrayFieldName);
    if (fieldNode instanceof ArrayNode) {
      // We already have an ArrayNode, no need to transform. This happens during `replace`
      // operations
      return transformedNode;
    }
    ObjectNode rebasedNode = transformedNode.deepCopy();
    ObjectNode mapNode = (ObjectNode) fieldNode;
    ArrayNode arrayNode;

    if (!keyFields.isEmpty()) {
      arrayNode = mergeToArray(mapNode, keyFields);
    } else {
      // No keys, assume pure Strings
      arrayNode = instance.arrayNode();
      mapNode.fields().forEachRemaining(entry -> arrayNode.add(entry.getValue()));
    }
    return rebasedNode.set(arrayFieldName, arrayNode);
  }

  default ArrayNode mergeToArray(JsonNode mapNode, List<String> keyFields) {
    if (keyFields.isEmpty()) {
      return instance.arrayNode().add(mapNode);
    } else {
      ArrayNode mergingArray = instance.arrayNode();
      mapNode
          .elements()
          .forEachRemaining(
              node ->
                  mergingArray.addAll(
                      mergeToArray(
                          node,
                          keyFields.size() > 1
                              ? keyFields.subList(1, keyFields.size())
                              : Collections.emptyList())));
      return mergingArray;
    }
  }
}
