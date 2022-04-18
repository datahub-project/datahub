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
public class DomainChangeEvent extends ChangeEvent {
  /**
   * The domain urn which was involved in the change event.
   */
  Urn domainUrn;

  @Builder(builderMethodName = "entityDomainChangeEventBuilder")
  public DomainChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      Urn domainUrn
  ) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of(
            "domainUrn", domainUrn.toString()
        ),
        auditStamp,
        semVerChange,
        description
    );
    this.domainUrn = domainUrn;
  }
}
