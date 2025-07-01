package com.linkedin.metadata.timeline.data.entity;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
@Getter
public class OwnerChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "entityOwnerChangeEventBuilder")
  public OwnerChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      Urn ownerUrn,
      OwnershipType ownerType,
      Urn ownerTypeUrn) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        buildParameters(ownerUrn, ownerType, ownerTypeUrn),
        auditStamp,
        semVerChange,
        description);
  }

  private static ImmutableMap<String, Object> buildParameters(
      Urn ownerUrn, OwnershipType ownerType, Urn ownerTypeUrn) {
    ImmutableMap.Builder<String, Object> builder =
        new ImmutableMap.Builder<String, Object>()
            .put("ownerUrn", ownerUrn.toString())
            .put("ownerType", ownerType.toString());
    if (ownerTypeUrn != null) {
      builder.put("ownerTypeUrn", ownerTypeUrn.toString());
    }

    return builder.build();
  }
}
