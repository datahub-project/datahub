package com.linkedin.metadata.timeline.data.entity;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
public class OwnerChangeEvent extends ChangeEvent {
  /**
   * The owner urn which was involved in the change event.
   */
  Urn ownerUrn;

  @Builder(builderMethodName = "entityOwnerChangeEventBuilder")
  public OwnerChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      Urn ownerUrn
  ) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of(
            "ownerUrn", ownerUrn.toString()
        ),
        auditStamp,
        semVerChange,
        description
    );
    this.ownerUrn = ownerUrn;
  }
}
