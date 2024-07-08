package com.linkedin.metadata.aspect.patch.template.structuredproperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.util.Collections;
import javax.annotation.Nonnull;

public class StructuredPropertyDefinitionTemplate
    extends CompoundKeyTemplate<StructuredPropertyDefinition> {

  private static final String ENTITY_TYPES_FIELD_NAME = "entityTypes";
  private static final String ALLOWED_VALUES_FIELD_NAME = "allowedValues";
  private static final String VALUE_FIELD_NAME = "value";
  private static final String UNIT_SEPARATOR_DELIMITER = "␟";

  @Override
  public StructuredPropertyDefinition getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof StructuredPropertyDefinition) {
      return (StructuredPropertyDefinition) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to StructuredPropertyDefinition");
  }

  @Override
  public Class<StructuredPropertyDefinition> getTemplateType() {
    return StructuredPropertyDefinition.class;
  }

  @Nonnull
  @Override
  public StructuredPropertyDefinition getDefault() {
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setQualifiedName("");
    definition.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    definition.setEntityTypes(new UrnArray());

    return definition;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(baseNode, ENTITY_TYPES_FIELD_NAME, Collections.emptyList());

    if (transformedNode.get(ALLOWED_VALUES_FIELD_NAME) == null) {
      return transformedNode;
    }

    // allowedValues has a nested key - value.string or value.number depending on type. Mapping
    // needs this nested key
    JsonNode allowedValues = transformedNode.get(ALLOWED_VALUES_FIELD_NAME);
    if (((ArrayNode) allowedValues).size() > 0) {
      JsonNode allowedValue = ((ArrayNode) allowedValues).get(0);
      JsonNode value = allowedValue.get(VALUE_FIELD_NAME);
      String secondaryKeyName = value.fieldNames().next();
      return arrayFieldToMap(
          transformedNode,
          ALLOWED_VALUES_FIELD_NAME,
          Collections.singletonList(
              VALUE_FIELD_NAME + UNIT_SEPARATOR_DELIMITER + secondaryKeyName));
    }

    return arrayFieldToMap(
        transformedNode, ALLOWED_VALUES_FIELD_NAME, Collections.singletonList(VALUE_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode patchedNode =
        transformedMapToArray(patched, ENTITY_TYPES_FIELD_NAME, Collections.emptyList());

    if (patchedNode.get(ALLOWED_VALUES_FIELD_NAME) == null) {
      return patchedNode;
    }
    return transformedMapToArray(
        patchedNode, ALLOWED_VALUES_FIELD_NAME, Collections.singletonList(VALUE_FIELD_NAME));
  }
}
