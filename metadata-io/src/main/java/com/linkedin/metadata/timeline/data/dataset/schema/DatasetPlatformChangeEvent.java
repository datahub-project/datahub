package com.linkedin.metadata.timeline.data.dataset.schema;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
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
public class DatasetPlatformChangeEvent extends ChangeEvent {

  @Builder(builderMethodName = "datasetPlatformChangeEventBuilder")
  public DatasetPlatformChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      Urn platform
  ) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of(
            "platform", platform
        ),
        auditStamp,
        semVerChange,
        description
    );
  }
}
