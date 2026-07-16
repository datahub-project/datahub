package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ArrayMergingTemplate<T extends RecordTemplate> extends Template<T> {

  Logger log = LoggerFactory.getLogger(ArrayMergingTemplate.class);

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
    // fieldNode is null when the entire array field was removed by the patch.
    // Return an empty array rather than NPE.
    if (fieldNode == null || fieldNode.isNull()) {
      return rebasedNode.set(arrayFieldName, instance.arrayNode());
    }
    ObjectNode mapNode = (ObjectNode) fieldNode;
    ArrayNode arrayNode;

    if (!keyFields.isEmpty()) {
      arrayNode = mergeToArray(mapNode, keyFields, resolveArrayItemSchema(arrayFieldName));
    } else {
      // No keys, assume pure Strings
      arrayNode = instance.arrayNode();
      mapNode.fields().forEachRemaining(entry -> arrayNode.add(entry.getValue()));
    }
    return rebasedNode.set(arrayFieldName, arrayNode);
  }

  default ArrayNode mergeToArray(
      JsonNode mapNode, List<String> keyFields, RecordDataSchema itemSchema) {
    if (keyFields.isEmpty()) {
      // When a plain add sets an array at an entity-key level (e.g. /tags/<urn> with
      // value [{...}]), the leaf ArrayNode's elements must be expanded into the result.
      if (mapNode instanceof ArrayNode) {
        return instance.arrayNode().addAll((ArrayNode) mapNode);
      }
      return instance.arrayNode().add(mapNode);
    }
    String keyField = keyFields.get(0);
    List<String> remainingKeyFields =
        keyFields.size() > 1 ? keyFields.subList(1, keyFields.size()) : Collections.emptyList();
    ArrayNode mergingArray = instance.arrayNode();
    if (mapNode instanceof ObjectNode) {
      mapNode
          .fields()
          .forEachRemaining(
              entry -> {
                ArrayNode merged = mergeToArray(entry.getValue(), remainingKeyFields, itemSchema);
                reinjectKey(merged, keyField, entry.getKey(), itemSchema);
                mergingArray.addAll(merged);
              });
    } else {
      // A patch can set a non-object (e.g. an array) at an intermediate key level. There is no
      // map key to restore here, so preserve the original value-only merge.
      mapNode
          .elements()
          .forEachRemaining(
              node -> mergingArray.addAll(mergeToArray(node, remainingKeyFields, itemSchema)));
    }
    return mergingArray;
  }

  // Best-effort resolution of the array element record schema. Returns null when it can't be
  // resolved, in which case keys are not re-injected (see reinjectKey); the debug logs make a
  // silent regression of that path traceable, as it is not expected for real aspects.
  default RecordDataSchema resolveArrayItemSchema(String arrayFieldName) {
    try {
      DataSchema aspectSchema = DataTemplateUtil.getSchema(getTemplateType());
      DataSchema fieldSchema =
          aspectSchema instanceof RecordDataSchema
              ? fieldSchemaOf((RecordDataSchema) aspectSchema, arrayFieldName)
              : null;
      if (!(fieldSchema instanceof ArrayDataSchema)) {
        log.debug(
            "Could not resolve keyed-array field {} on aspect {}; skipping key re-injection",
            arrayFieldName,
            getTemplateType());
        return null;
      }
      DataSchema itemSchema =
          ((ArrayDataSchema) fieldSchema).getItems().getDereferencedDataSchema();
      return itemSchema instanceof RecordDataSchema ? (RecordDataSchema) itemSchema : null;
    } catch (RuntimeException e) {
      log.debug(
          "Failed to resolve schema for field {} on aspect {}; skipping key re-injection",
          arrayFieldName,
          getTemplateType(),
          e);
      return null;
    }
  }

  private static DataSchema fieldSchemaOf(RecordDataSchema recordSchema, String fieldName) {
    RecordDataSchema.Field field = recordSchema.getField(fieldName);
    return field == null ? null : field.getType().getDereferencedDataSchema();
  }

  // Restores a key field that only lived in the patch path (e.g. a deep-path add) and would
  // otherwise be dropped. Only primitive/enum keys are restored: the key is a scalar path segment,
  // so injecting it into a record- or collection-typed key field would corrupt the element.
  private static void reinjectKey(
      ArrayNode elements, String keyField, String key, RecordDataSchema itemSchema) {
    if (key == null
        || key.isEmpty()
        || keyField.contains(UNIT_SEPARATOR_DELIMITER)
        || !isScalarKeyField(itemSchema, keyField)) {
      return;
    }
    elements.forEach(
        element -> {
          if (element instanceof ObjectNode && element.get(keyField) == null) {
            ((ObjectNode) element).set(keyField, TextNode.valueOf(key));
          }
        });
  }

  private static boolean isScalarKeyField(RecordDataSchema itemSchema, String keyField) {
    if (itemSchema == null) {
      return false;
    }
    RecordDataSchema.Field field = itemSchema.getField(keyField);
    if (field == null) {
      return false;
    }
    switch (field.getType().getDereferencedType()) {
      case STRING:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case ENUM:
        return true;
      default:
        return false;
    }
  }
}
