package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class StructuredPropertyDefinitionPatchBuilder
    extends AbstractMultiFieldPatchBuilder<StructuredPropertyDefinitionPatchBuilder> {

  public static final String PATH_DELIM = "/";
  public static final String QUALIFIED_NAME_FIELD = "qualifiedName";
  public static final String DISPLAY_NAME_FIELD = "displayName";
  public static final String VALUE_TYPE_FIELD = "valueType";
  public static final String TYPE_QUALIFIER_FIELD = "typeQualifier";
  public static final String ALLOWED_VALUES_FIELD = "allowedValues";
  public static final String CARDINALITY_FIELD = "cardinality";
  public static final String ENTITY_TYPES_FIELD = "entityTypes";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String IMMUTABLE_FIELD = "immutable";
  private static final String LAST_MODIFIED_KEY = "lastModified";
  private static final String CREATED_KEY = "created";
  private static final String TIME_KEY = "time";
  private static final String ACTOR_KEY = "actor";

  // can only be used when creating a new structured property
  public StructuredPropertyDefinitionPatchBuilder setQualifiedName(@Nonnull String name) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + QUALIFIED_NAME_FIELD,
            instance.textNode(name)));
    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder setDisplayName(@Nonnull String displayName) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + DISPLAY_NAME_FIELD,
            instance.textNode(displayName)));
    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder setValueType(@Nonnull String valueTypeUrn) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + VALUE_TYPE_FIELD,
            instance.textNode(valueTypeUrn)));
    return this;
  }

  // can only be used when creating a new structured property
  public StructuredPropertyDefinitionPatchBuilder setTypeQualifier(
      @Nonnull StringArrayMap typeQualifier) {
    ObjectNode value = instance.objectNode();
    typeQualifier.forEach(
        (key, values) -> {
          ArrayNode valuesNode = instance.arrayNode();
          values.forEach(valuesNode::add);
          value.set(key, valuesNode);
        });
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), PATH_DELIM + TYPE_QUALIFIER_FIELD, value));
    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder addAllowedValue(
      @Nonnull PropertyValue propertyValue) {
    try {
      ObjectNode valueNode =
          (ObjectNode) new ObjectMapper().readTree(RecordUtils.toJsonString(propertyValue));
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              PATH_DELIM + ALLOWED_VALUES_FIELD + PATH_DELIM + propertyValue.getValue(),
              valueNode));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to add allowed value, failed to parse provided aspect json.", e);
    }
  }

  public StructuredPropertyDefinitionPatchBuilder setCardinality(
      @Nonnull PropertyCardinality cardinality) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + CARDINALITY_FIELD,
            instance.textNode(cardinality.toString())));
    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder addEntityType(@Nonnull String entityTypeUrn) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + ENTITY_TYPES_FIELD + PATH_DELIM + entityTypeUrn,
            instance.textNode(entityTypeUrn)));
    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder setDescription(@Nullable String description) {
    if (description == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), PATH_DELIM + DESCRIPTION_FIELD, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              PATH_DELIM + DESCRIPTION_FIELD,
              instance.textNode(description)));
    }
    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder setImmutable(boolean immutable) {
    this.pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PATH_DELIM + IMMUTABLE_FIELD,
            instance.booleanNode(immutable)));
    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder setLastModified(
      @Nonnull AuditStamp lastModified) {
    ObjectNode lastModifiedValue = instance.objectNode();
    lastModifiedValue.put(TIME_KEY, lastModified.getTime());
    lastModifiedValue.put(ACTOR_KEY, lastModified.getActor().toString());

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), "/" + LAST_MODIFIED_KEY, lastModifiedValue));

    return this;
  }

  public StructuredPropertyDefinitionPatchBuilder setCreated(@Nonnull AuditStamp created) {
    ObjectNode createdValue = instance.objectNode();
    createdValue.put(TIME_KEY, created.getTime());
    createdValue.put(ACTOR_KEY, created.getActor().toString());

    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), "/" + CREATED_KEY, createdValue));

    return this;
  }

  @Override
  protected String getAspectName() {
    return STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return STRUCTURED_PROPERTY_ENTITY_NAME;
  }
}
