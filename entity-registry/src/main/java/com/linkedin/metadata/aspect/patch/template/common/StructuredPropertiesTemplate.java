package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nonnull;

public class StructuredPropertiesTemplate implements ArrayMergingTemplate<StructuredProperties> {

  private static final String PROPERTIES_FIELD_NAME = "properties";
  private static final String URN_FIELD_NAME = "propertyUrn";
  private static final String ATTRIBUTION_SOURCE =
      "attribution" + UNIT_SEPARATOR_DELIMITER + "source";

  @Override
  public StructuredProperties getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof StructuredProperties) {
      return (StructuredProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to StructuredProperties");
  }

  @Override
  public Class<StructuredProperties> getTemplateType() {
    return StructuredProperties.class;
  }

  @Nonnull
  @Override
  public StructuredProperties getDefault() {
    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(new StructuredPropertyValueAssignmentArray());
    return structuredProperties;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(
        baseNode,
        PROPERTIES_FIELD_NAME,
        Collections.unmodifiableList(Arrays.asList(URN_FIELD_NAME, ATTRIBUTION_SOURCE)));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        patched,
        PROPERTIES_FIELD_NAME,
        Collections.unmodifiableList(Arrays.asList(URN_FIELD_NAME, ATTRIBUTION_SOURCE)));
  }
}
