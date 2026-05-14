package com.linkedin.metadata.timeline.data.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@Getter
public class StructuredPropertyAssignmentChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "entityStructuredPropertyAssignmentChangeEventBuilder")
  public StructuredPropertyAssignmentChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      StructuredPropertyValueAssignment structuredPropertyValueAssignment) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        buildParameters(structuredPropertyValueAssignment),
        auditStamp,
        semVerChange,
        description);
  }

  private static ImmutableMap<String, Object> buildParameters(
      StructuredPropertyValueAssignment structuredPropertyValueAssignment) {

    ObjectMapper objectMapper = new ObjectMapper();
    ArrayNode arrayNode = objectMapper.createArrayNode();

    structuredPropertyValueAssignment
        .getValues()
        .forEach(
            value -> {
              if (value == null) {
                throw new IllegalArgumentException(
                    "StructuredPropertyValueAssignment values cannot be null");
              }
              if (value.isString()) {
                arrayNode.add(value.getString());
              } else if (value.isDouble()) {
                arrayNode.add(value.getDouble());
              } else if (value.isNull()) {
                arrayNode.addNull();
              } else {
                throw new RuntimeException("Unsupported structured property value type: " + value);
              }
            });

    ImmutableMap.Builder<String, Object> builder =
        new ImmutableMap.Builder<String, Object>()
            .put("propertyUrn", structuredPropertyValueAssignment.getPropertyUrn().toString())
            .put("propertyValues", arrayNode.toString());
    return builder.build();
  }
}
