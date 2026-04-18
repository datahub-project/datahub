package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.Collections;
import javax.annotation.Nonnull;

public class StructuredPropertiesTemplate implements ArrayMergingTemplate<StructuredProperties> {

  private static final String PROPERTIES_FIELD_NAME = "properties";
  private static final String URN_FIELD_NAME = "propertyUrn";

  //  private static final String AUDIT_STAMP_FIELD = "auditStamp";
  //  private static final String TIME_FIELD = "time";
  //  private static final String ACTOR_FIELD = "actor";

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
    //    .setAuditStamp(new
    // AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));
    return structuredProperties;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(
        baseNode, PROPERTIES_FIELD_NAME, Collections.singletonList(URN_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        patched, PROPERTIES_FIELD_NAME, Collections.singletonList(URN_FIELD_NAME));
  }
}
