package com.linkedin.metadata.search.transformer;

import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.extractor.FieldExtractor;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;


/**
 * Class that provides a utility function that transforms the snapshot object into a search document
 */
@Slf4j
public class SearchDocumentTransformer {

  private SearchDocumentTransformer() {
  }

  public static Optional<String> transformSnapshot(
      final RecordTemplate snapshot,
      final EntitySpec entitySpec,
      final Boolean forDelete
  ) {
    final Map<SearchableFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFieldsFromSnapshot(snapshot, entitySpec, AspectSpec::getSearchableFieldSpecs);
    if (extractedFields.isEmpty()) {
      return Optional.empty();
    }
    final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    searchDocument.put("urn", snapshot.data().get("urn").toString());
    extractedFields.forEach((key, value) -> setValue(key, value, searchDocument, forDelete));
    return Optional.of(searchDocument.toString());
  }

  public static Optional<String> transformAspect(
      final Urn urn,
      final RecordTemplate aspect,
      final AspectSpec aspectSpec,
      final Boolean forDelete
  ) {
    final Map<SearchableFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getSearchableFieldSpecs());
    if (extractedFields.isEmpty()) {
      return Optional.empty();
    }
    final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    searchDocument.put("urn", urn.toString());
    extractedFields.forEach((key, value) -> setValue(key, value, searchDocument, forDelete));
    return Optional.of(searchDocument.toString());
  }

  public static void setValue(final SearchableFieldSpec fieldSpec, final List<Object> fieldValues,
      final ObjectNode searchDocument, final Boolean forDelete) {
    DataSchema.Type valueType = fieldSpec.getPegasusSchema().getType();
    Optional<Object> firstValue = fieldValues.stream().findFirst();
    boolean isArray = fieldSpec.isArray();

    // Set hasValues field if exists
    fieldSpec.getSearchableAnnotation().getHasValuesFieldName().ifPresent(fieldName -> {
      if (forDelete) {
        searchDocument.set(fieldName, JsonNodeFactory.instance.booleanNode(false));
        return;
      }
      if (valueType == DataSchema.Type.BOOLEAN) {
        searchDocument.set(fieldName, JsonNodeFactory.instance.booleanNode((Boolean) firstValue.orElse(false)));
      } else {
        searchDocument.set(fieldName, JsonNodeFactory.instance.booleanNode(!fieldValues.isEmpty()));
      }
    });

    // Set numValues field if exists
    fieldSpec.getSearchableAnnotation().getNumValuesFieldName().ifPresent(fieldName -> {
      if (forDelete) {
        searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Integer) 0));
        return;
      }
      switch (valueType) {
        case INT:
          searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Integer) firstValue.orElse(0)));
          break;
        case LONG:
          searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode((Long) firstValue.orElse(0L)));
          break;
        default:
          searchDocument.set(fieldName, JsonNodeFactory.instance.numberNode(fieldValues.size()));
          break;
      }
    });

    final String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
    final FieldType fieldType = fieldSpec.getSearchableAnnotation().getFieldType();

    if (forDelete) {
      searchDocument.set(fieldName, JsonNodeFactory.instance.nullNode());
      return;
    }

    if (isArray || valueType == DataSchema.Type.MAP) {
      ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
      fieldValues.forEach(value -> getNodeForValue(valueType, value, fieldType).ifPresent(arrayNode::add));
      searchDocument.set(fieldName, arrayNode);
    } else if (!fieldValues.isEmpty()) {
      getNodeForValue(valueType, fieldValues.get(0), fieldType).ifPresent(node -> searchDocument.set(fieldName, node));
    }
  }

  private static Optional<JsonNode> getNodeForValue(final DataSchema.Type schemaFieldType, final Object fieldValue,
      final FieldType fieldType) {
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
        return value.isEmpty() ? Optional.empty()
            : Optional.of(JsonNodeFactory.instance.textNode(fieldValue.toString()));
    }
  }
}
