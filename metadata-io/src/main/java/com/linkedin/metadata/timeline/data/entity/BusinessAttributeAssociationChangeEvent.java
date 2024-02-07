package com.linkedin.metadata.timeline.data.entity;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@Getter
public class BusinessAttributeAssociationChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "entityBusinessAttributeAssociationChangeEventBuilder")
  public BusinessAttributeAssociationChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      Map<String, Object> parameters,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      Urn businessAttributeUrn) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of("businessAttributeUrn", businessAttributeUrn.toString()),
        auditStamp,
        semVerChange,
        description);
  }
}
