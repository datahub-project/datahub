package com.linkedin.metadata.search.transformer;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.models.StructuredPropertyUtils.sanitizeStructuredPropertyFQN;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.MappingsBuilder.SYSTEM_CREATED_FIELD;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.validation.StructuredPropertiesValidator;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  private AspectRetriever aspectRetriever;

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

  public static ObjectNode withSystemCreated(
      ObjectNode searchDocument,
      @Nonnull ChangeType changeType,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull final AuditStamp auditStamp) {

    // relies on the MCP processor preventing unneeded key aspects
    if (Set.of(ChangeType.CREATE, ChangeType.CREATE_ENTITY, ChangeType.UPSERT).contains(changeType)
        && entitySpec.getKeyAspectName().equals(aspectSpec.getName())) {
      searchDocument.put(SYSTEM_CREATED_FIELD, auditStamp.getTime());
    }
    return searchDocument;
  }

  public Optional<ObjectNode> transformAspect(
      final @Nonnull Urn urn,
      final @Nonnull RecordTemplate aspect,
      final @Nonnull AspectSpec aspectSpec,
      final Boolean forDelete)
      throws RemoteInvocationException, URISyntaxException {
    final Map<SearchableFieldSpec, List<Object>> extractedSearchableFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getSearchableFieldSpecs(), maxValueLength);
    final Map<SearchScoreFieldSpec, List<Object>> extractedSearchScoreFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getSearchScoreFieldSpecs(), maxValueLength);

    Optional<ObjectNode> result = Optional.empty();

    if (!extractedSearchableFields.isEmpty() || !extractedSearchScoreFields.isEmpty()) {
      final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
      searchDocument.put("urn", urn.toString());

      extractedSearchableFields.forEach(
          (key, values) -> setSearchableValue(key, values, searchDocument, forDelete));
      extractedSearchScoreFields.forEach(
          (key, values) -> setSearchScoreValue(key, values, searchDocument, forDelete));
      result = Optional.of(searchDocument);
    } else if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName())) {
      final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
      searchDocument.put("urn", urn.toString());
      setStructuredPropertiesSearchValue(
          new StructuredProperties(aspect.data()), searchDocument, forDelete);
      result = Optional.of(searchDocument);
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

    if (isArray || (valueType == DataSchema.Type.MAP && !OBJECT_FIELD_TYPES.contains(fieldType))) {
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
    } else if (valueType == DataSchema.Type.MAP && FieldType.MAP_ARRAY.equals(fieldType)) {
      ObjectNode dictDoc = JsonNodeFactory.instance.objectNode();
      fieldValues
          .subList(0, Math.min(fieldValues.size(), maxObjectKeys))
          .forEach(
              fieldValue -> {
                String[] keyValues = fieldValue.toString().split("=");
                String key = keyValues[0];
                ArrayNode values = JsonNodeFactory.instance.arrayNode();
                Arrays.stream(keyValues[1].substring(1, keyValues[1].length() - 1).split(", "))
                    .forEach(
                        v -> {
                          if (!v.isEmpty()) {
                            values.add(v);
                          }
                        });
                dictDoc.set(key, values);
              });
      searchDocument.set(fieldName, dictDoc);
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

  private void setStructuredPropertiesSearchValue(
      final StructuredProperties values, final ObjectNode searchDocument, final Boolean forDelete)
      throws RemoteInvocationException, URISyntaxException {
    Map<Urn, Set<StructuredPropertyValueAssignment>> propertyMap =
        values.getProperties().stream()
            .collect(
                Collectors.groupingBy(
                    StructuredPropertyValueAssignment::getPropertyUrn, Collectors.toSet()));

    Map<Urn, Map<String, Aspect>> definitions =
        aspectRetriever.getLatestAspectObjects(
            propertyMap.keySet(), Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));

    if (definitions.size() < propertyMap.size()) {
      String message =
          String.format(
              "Missing property definitions. %s",
              propertyMap.keySet().stream()
                  .filter(k -> !definitions.containsKey(k))
                  .collect(Collectors.toSet()));
      log.error(message);
    }

    propertyMap
        .entrySet()
        .forEach(
            propertyEntry -> {
              StructuredPropertyDefinition definition =
                  new StructuredPropertyDefinition(
                      definitions
                          .get(propertyEntry.getKey())
                          .get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
                          .data());
              String fieldName =
                  String.join(
                      ".",
                      List.of(
                          STRUCTURED_PROPERTY_MAPPING_FIELD,
                          sanitizeStructuredPropertyFQN(definition.getQualifiedName())));

              if (forDelete) {
                searchDocument.set(fieldName, JsonNodeFactory.instance.nullNode());
              } else {
                LogicalValueType logicalValueType =
                    StructuredPropertiesValidator.getLogicalValueType(definition.getValueType());

                ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();

                propertyEntry
                    .getValue()
                    .forEach(
                        property ->
                            property
                                .getValues()
                                .forEach(
                                    propertyValue -> {
                                      final Optional<JsonNode> searchValue;
                                      switch (logicalValueType) {
                                        case UNKNOWN:
                                          log.warn(
                                              "Unable to transform UNKNOWN logical value type.");
                                          searchValue = Optional.empty();
                                          break;
                                        case NUMBER:
                                          Double doubleValue =
                                              propertyValue.getDouble() != null
                                                  ? propertyValue.getDouble()
                                                  : Double.valueOf(propertyValue.getString());
                                          searchValue =
                                              Optional.of(
                                                  JsonNodeFactory.instance.numberNode(doubleValue));
                                          break;
                                        default:
                                          searchValue =
                                              propertyValue.getString().isEmpty()
                                                  ? Optional.empty()
                                                  : Optional.of(
                                                      JsonNodeFactory.instance.textNode(
                                                          propertyValue.getString()));
                                          break;
                                      }
                                      searchValue.ifPresent(arrayNode::add);
                                    }));

                searchDocument.set(fieldName, arrayNode);
              }
            });
  }
}
