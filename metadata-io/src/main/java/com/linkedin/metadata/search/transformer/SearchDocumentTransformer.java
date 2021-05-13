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

  public static JsonNode transform(final RecordTemplate snapshot, final EntitySpec entitySpec) {
    final Map<String, List<SearchableFieldSpec>> searchableFieldSpecsPerAspect = entitySpec.getAspectSpecMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getSearchableFieldSpecs()));
    final Map<SearchableFieldSpec, Optional<Object>> extractedFields =
        FieldExtractor.extractFields(snapshot, searchableFieldSpecsPerAspect);
    final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    searchDocument.put("urn", snapshot.data().get("urn").toString());
    extractedFields.forEach((key, value) -> setValue(key, value, searchDocument));
    return searchDocument;
  }

  public static void setValue(final SearchableFieldSpec fieldSpec, final Optional<Object> fieldValueOpt,
      ObjectNode searchDocument) {
    // Separate the settings with override and settings without
    Map<Boolean, List<IndexSetting>> indexSettingsHasOverride = fieldSpec.getIndexSettings()
        .stream()
        .collect(Collectors.partitioningBy(setting -> setting.getOverrideFieldName().isPresent()));
    DataSchema.Type valueType = fieldSpec.getPegasusSchema().getType();

    //
    for (IndexSetting indexSetting : indexSettingsHasOverride.getOrDefault(true, ImmutableList.of())) {
      JsonNode valueNode = null;
      if (indexSetting.getIndexType() == IndexType.BOOLEAN) {
        switch (valueType) {
          case BOOLEAN:
            valueNode = JsonNodeFactory.instance.booleanNode((Boolean) fieldValueOpt.orElse(false));
            break;
          case ARRAY:
            // If array, check if array is set and is not empty
            valueNode = JsonNodeFactory.instance.booleanNode(
                fieldValueOpt.isPresent() && !((List<?>) fieldValueOpt.get()).isEmpty());
            break;
          default:
            valueNode = JsonNodeFactory.instance.booleanNode(fieldValueOpt.isPresent());
            break;
        }
      } else if (indexSetting.getIndexType() == IndexType.COUNT) {
        switch (valueType) {
          case INT:
            valueNode = JsonNodeFactory.instance.numberNode((Integer) fieldValueOpt.orElse(0));
            break;
          case LONG:
            valueNode = JsonNodeFactory.instance.numberNode((Long) fieldValueOpt.orElse(0L));
            break;
          case ARRAY:
            // If array, check length of array
            valueNode =
                JsonNodeFactory.instance.numberNode(((List<?>) fieldValueOpt.orElse(ImmutableList.of())).size());
            break;
          default:
            log.info("Non-countable fields are not supported for count index: {}", valueType);
            break;
        }
      }
      String overrideFieldName = indexSetting.getOverrideFieldName().get();
      // If any of the above special cases matched, set the overriden field name
      if (valueNode != null) {
        searchDocument.set(overrideFieldName, valueNode);
      } else {
        fieldValueOpt.ifPresent(
            fieldValue -> searchDocument.set(overrideFieldName, getNodeForValue(valueType, fieldValue)));
      }
    }

    if (!indexSettingsHasOverride.containsKey(false) || indexSettingsHasOverride.get(false).isEmpty()
        || !fieldValueOpt.isPresent()) {
      return;
    }
    // For fields with an index setting without an override, set key to fieldName
    final String fieldName = fieldSpec.getFieldName();
    JsonNode valueNode = getNodeForValue(valueType, fieldValueOpt.get());
    searchDocument.set(fieldName, valueNode);
  }

  public static JsonNode getNodeForValue(final DataSchema.Type fieldType, final Object fieldValue) {
    switch (fieldType) {
      case BOOLEAN:
        return JsonNodeFactory.instance.booleanNode((Boolean) fieldValue);
      case INT:
        return JsonNodeFactory.instance.numberNode((Integer) fieldValue);
      case LONG:
        return JsonNodeFactory.instance.numberNode((Long) fieldValue);
      case ARRAY:
        List<Object> valueList = (List<Object>) fieldValue;
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        valueList.forEach(value -> arrayNode.add(value.toString()));
        return arrayNode;
      // By default run toString
      default:
        return JsonNodeFactory.instance.textNode(fieldValue.toString());
    }
  }
}
