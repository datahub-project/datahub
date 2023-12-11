package com.linkedin.metadata.search.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Class that provides a utility function that transforms the snapshot object into a search document
 */
@Slf4j
@Setter
@RequiredArgsConstructor
public class SearchDocumentTransformer {

  // Number of elements to index for a given array.
  // The cap improves search speed when having fields with a large number of elements
  private final int maxArrayLength;

  private final int maxObjectKeys;

  // Maximum customProperties value length
  private final int maxValueLength;

  private SystemEntityClient entityClient;

  private static final String BROWSE_PATH_V2_DELIMITER = "␟";

  public Optional<String> transformSnapshot(
      final RecordTemplate snapshot, final EntitySpec entitySpec, final Boolean forDelete) {
    final Map<SearchableFieldSpec, List<Object>> extractedSearchableFields =
        FieldExtractor.extractFieldsFromSnapshot(
                snapshot, entitySpec, AspectSpec::getSearchableFieldSpecs, maxValueLength)
            .entrySet()
            // Delete expects urn to be preserved
            .stream()
            .filter(
                entry ->
                    !forDelete
                        || !"urn".equals(entry.getKey().getSearchableAnnotation().getFieldName()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    final Map<SearchScoreFieldSpec, List<Object>> extractedSearchScoreFields =
        FieldExtractor.extractFieldsFromSnapshot(
            snapshot, entitySpec, AspectSpec::getSearchScoreFieldSpecs, maxValueLength);
    if (extractedSearchableFields.isEmpty() && extractedSearchScoreFields.isEmpty()) {
      return Optional.empty();
    }
    final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    searchDocument.put("urn", snapshot.data().get("urn").toString());
    extractedSearchableFields.forEach(
        (key, value) -> setSearchableValue(key, value, searchDocument, forDelete));
    extractedSearchScoreFields.forEach(
        (key, values) -> setSearchScoreValue(key, values, searchDocument, forDelete));
    return Optional.of(searchDocument.toString());
  }

  public Optional<String> transformAspect(
      final Urn urn,
      final RecordTemplate aspect,
      final AspectSpec aspectSpec,
      final Boolean forDelete) {
    final Map<SearchableFieldSpec, List<Object>> extractedSearchableFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getSearchableFieldSpecs(), maxValueLength);
    final Map<SearchScoreFieldSpec, List<Object>> extractedSearchScoreFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getSearchScoreFieldSpecs(), maxValueLength);

    Optional<String> result = Optional.empty();

    if (!extractedSearchableFields.isEmpty() || !extractedSearchScoreFields.isEmpty()) {
      final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
      searchDocument.put("urn", urn.toString());
      extractedSearchableFields.forEach(
          (key, values) -> setSearchableValue(key, values, searchDocument, forDelete));
      extractedSearchScoreFields.forEach(
          (key, values) -> setSearchScoreValue(key, values, searchDocument, forDelete));
      result = Optional.of(searchDocument.toString());
    }

    return result;
  }

  public void setSearchableValue(
      final SearchableFieldSpec fieldSpec,
      final List<Object> fieldValues,
      final ObjectNode searchDocument,
      final Boolean forDelete) {
    DataSchema.Type valueType = fieldSpec.getPegasusSchema().getType();
    Optional<Object> firstValue = fieldValues.stream().findFirst();
    boolean isArray = fieldSpec.isArray();

    // Set hasValues field if exists
    fieldSpec
        .getSearchableAnnotation()
        .getHasValuesFieldName()
        .ifPresent(
            fieldName -> {
              if (forDelete) {
                searchDocument.set(fieldName, JsonNodeFactory.instance.booleanNode(false));
                return;
              }
              if (valueType == DataSchema.Type.BOOLEAN) {
                searchDocument.set(
                    fieldName,
                    JsonNodeFactory.instance.booleanNode((Boolean) firstValue.orElse(false)));
              } else {
                searchDocument.set(
                    fieldName, JsonNodeFactory.instance.booleanNode(!fieldValues.isEmpty()));
              }
            });

    // Set numValues field if exists
    fieldSpec
        .getSearchableAnnotation()
        .getNumValuesFieldName()
        .ifPresent(
            fieldName -> {
              if (forDelete) {
                searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Integer) 0));
                return;
              }
              switch (valueType) {
                case INT:
                  searchDocument.set(
                      fieldName,
                      JsonNodeFactory.instance.numberNode((Integer) firstValue.orElse(0)));
                  break;
                case LONG:
                  searchDocument.set(
                      fieldName, JsonNodeFactory.instance.numberNode((Long) firstValue.orElse(0L)));
                  break;
                default:
                  searchDocument.set(
                      fieldName, JsonNodeFactory.instance.numberNode(fieldValues.size()));
                  break;
              }
            });

    final String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
    final FieldType fieldType = fieldSpec.getSearchableAnnotation().getFieldType();

    if (forDelete) {
      searchDocument.set(fieldName, JsonNodeFactory.instance.nullNode());
      return;
    }

    if (isArray || (valueType == DataSchema.Type.MAP && fieldType != FieldType.OBJECT)) {
      if (fieldType == FieldType.BROWSE_PATH_V2) {
        String browsePathV2Value = getBrowsePathV2Value(fieldValues);
        searchDocument.set(fieldName, JsonNodeFactory.instance.textNode(browsePathV2Value));
      } else {
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        fieldValues
            .subList(0, Math.min(fieldValues.size(), maxArrayLength))
            .forEach(
                value -> getNodeForValue(valueType, value, fieldType).ifPresent(arrayNode::add));
        searchDocument.set(fieldName, arrayNode);
      }
    } else if (valueType == DataSchema.Type.MAP) {
      ObjectNode dictDoc = JsonNodeFactory.instance.objectNode();
      fieldValues
          .subList(0, Math.min(fieldValues.size(), maxObjectKeys))
          .forEach(
              fieldValue -> {
                String[] keyValues = fieldValue.toString().split("=");
                String key = keyValues[0];
                String value = keyValues[1];
                dictDoc.put(key, value);
              });
      searchDocument.set(fieldName, dictDoc);
    } else if (!fieldValues.isEmpty()) {
      getNodeForValue(valueType, fieldValues.get(0), fieldType)
          .ifPresent(node -> searchDocument.set(fieldName, node));
    }
  }

  public void setSearchScoreValue(
      final SearchScoreFieldSpec fieldSpec,
      final List<Object> fieldValues,
      final ObjectNode searchDocument,
      final Boolean forDelete) {
    DataSchema.Type valueType = fieldSpec.getPegasusSchema().getType();

    final String fieldName = fieldSpec.getSearchScoreAnnotation().getFieldName();

    if (forDelete) {
      searchDocument.set(fieldName, JsonNodeFactory.instance.nullNode());
      return;
    }

    if (fieldValues.isEmpty()) {
      return;
    }

    final Object fieldValue = fieldValues.get(0);
    switch (valueType) {
      case INT:
        searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Integer) fieldValue));
        return;
      case LONG:
        searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Long) fieldValue));
        return;
      case FLOAT:
        searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Float) fieldValue));
        return;
      case DOUBLE:
        searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Double) fieldValue));
        return;
      default:
        // Only the above types are supported
        throw new IllegalArgumentException(
            String.format(
                "SearchScore fields must be a numeric type: field %s, value %s",
                fieldName, fieldValue));
    }
  }

  private Optional<JsonNode> getNodeForValue(
      final DataSchema.Type schemaFieldType, final Object fieldValue, final FieldType fieldType) {
    switch (schemaFieldType) {
      case BOOLEAN:
        return Optional.of(JsonNodeFactory.instance.booleanNode((Boolean) fieldValue));
      case INT:
        return Optional.of(JsonNodeFactory.instance.numberNode((Integer) fieldValue));
      case LONG:
        return Optional.of(JsonNodeFactory.instance.numberNode((Long) fieldValue));
        // By default run toString
      default:
        String value = fieldValue.toString();
        // If index type is BROWSE_PATH, make sure the value starts with a slash
        if (fieldType == FieldType.BROWSE_PATH && !value.startsWith("/")) {
          value = "/" + value;
        }
        return value.isEmpty()
            ? Optional.empty()
            : Optional.of(JsonNodeFactory.instance.textNode(fieldValue.toString()));
    }
  }

  /**
   * The browsePathsV2 aspect is a list of objects and the @Searchable annotation specifies a list
   * of strings that we receive. However, we want to aggregate those strings and store as a single
   * string in ElasticSearch so we can do prefix matching against it.
   */
  private String getBrowsePathV2Value(@Nonnull final List<Object> fieldValues) {
    List<String> stringValues = new ArrayList<>();
    fieldValues
        .subList(0, Math.min(fieldValues.size(), maxArrayLength))
        .forEach(
            value -> {
              if (value instanceof String) {
                stringValues.add((String) value);
              }
            });
    String aggregatedValue = String.join(BROWSE_PATH_V2_DELIMITER, stringValues);
    // ensure browse path v2 starts with our delimiter if it's not empty
    if (!aggregatedValue.equals("") && !aggregatedValue.startsWith(BROWSE_PATH_V2_DELIMITER)) {
      aggregatedValue = BROWSE_PATH_V2_DELIMITER + aggregatedValue;
    }
    return aggregatedValue;
  }
}
