package com.linkedin.metadata.search.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.extractor.FieldExtractor;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexSetting;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


/**
 * Class that provides a utility function that transforms the snapshot object into a search document
 */
@Slf4j
public class SearchDocumentTransformer {

  private SearchDocumentTransformer() {
  }

  public static Optional<JsonNode> transform(final RecordTemplate snapshot, final EntitySpec entitySpec) {
    final Map<String, List<SearchableFieldSpec>> searchableFieldSpecsPerAspect = entitySpec.getAspectSpecMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getSearchableFieldSpecs()));
    final Map<SearchableFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(snapshot, searchableFieldSpecsPerAspect);
    if (extractedFields.isEmpty()) {
      return Optional.empty();
    }
    final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    searchDocument.put("urn", snapshot.data().get("urn").toString());
    extractedFields.forEach((key, value) -> setValue(key, value, searchDocument));
    return Optional.of(searchDocument);
  }

  public static void setValue(final SearchableFieldSpec fieldSpec, final List<Object> fieldValues,
      final ObjectNode searchDocument) {
    // Separate the settings with override and settings without
    Map<Boolean, List<IndexSetting>> indexSettingsHasOverride = fieldSpec.getIndexSettings()
        .stream()
        .collect(Collectors.partitioningBy(setting -> setting.getOverrideFieldName().isPresent()));
    DataSchema.Type valueType = fieldSpec.getPegasusSchema().getType();
    Optional<Object> firstValue = fieldValues.stream().findFirst();
    boolean isArray = fieldSpec.isArray();

    // Deal with settings with overrides first
    for (IndexSetting indexSetting : indexSettingsHasOverride.getOrDefault(true, ImmutableList.of())) {
      JsonNode valueNode = null;
      if (indexSetting.getIndexType() == IndexType.BOOLEAN) {
        if (valueType == DataSchema.Type.BOOLEAN) {
          valueNode = JsonNodeFactory.instance.booleanNode((Boolean) firstValue.orElse(false));
        } else {
          valueNode = JsonNodeFactory.instance.booleanNode(!fieldValues.isEmpty());
        }
      } else if (indexSetting.getIndexType() == IndexType.COUNT) {
        switch (valueType) {
          case INT:
            valueNode = JsonNodeFactory.instance.numberNode((Integer) firstValue.orElse(0));
            break;
          case LONG:
            valueNode = JsonNodeFactory.instance.numberNode((Long) firstValue.orElse(0L));
            break;
          default:
            valueNode = JsonNodeFactory.instance.numberNode(fieldValues.size());
            break;
        }
      } else {
        Optional<JsonNode> valueNodeOpt = getNodeForValue(valueType, fieldValues, isArray);
        if (valueNodeOpt.isPresent()) {
          valueNode = valueNodeOpt.get();
        }
      }
      String overrideFieldName = indexSetting.getOverrideFieldName().get();
      // If any of the above special cases matched, set the overriden field name
      if (valueNode != null) {
        searchDocument.set(overrideFieldName, valueNode);
      }
    }

    if (!indexSettingsHasOverride.containsKey(false) || indexSettingsHasOverride.get(false).isEmpty()) {
      return;
    }
    // For fields with an index setting without an override, set key to fieldName
    final String fieldName = fieldSpec.getFieldName();
    Optional<JsonNode> valueNode = getNodeForValue(valueType, fieldValues, isArray);
    valueNode.ifPresent(node -> searchDocument.set(fieldName, node));
  }

  private static Optional<JsonNode> getNodeForValue(final DataSchema.Type fieldType, final List<Object> fieldValues,
      final boolean isArray) {
    if (isArray) {
      ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
      fieldValues.forEach(value -> getNodeForValue(fieldType, value).ifPresent(arrayNode::add));
      return Optional.of(arrayNode);
    }
    if (!fieldValues.isEmpty()) {
      return getNodeForValue(fieldType, fieldValues.get(0));
    }
    return Optional.empty();
  }

  private static Optional<JsonNode> getNodeForValue(final DataSchema.Type fieldType, final Object fieldValue) {
    switch (fieldType) {
      case BOOLEAN:
        return Optional.of(JsonNodeFactory.instance.booleanNode((Boolean) fieldValue));
      case INT:
        return Optional.of(JsonNodeFactory.instance.numberNode((Integer) fieldValue));
      case LONG:
        return Optional.of(JsonNodeFactory.instance.numberNode((Long) fieldValue));
      // By default run toString
      default:
        String value = fieldValue.toString();
        return value.isEmpty() ? Optional.empty()
            : Optional.of(JsonNodeFactory.instance.textNode(fieldValue.toString()));
    }
  }
}
