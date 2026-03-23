package com.linkedin.metadata.timeline.data.entity;

import com.linkedin.common.AuditStamp;
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
public class GlossaryTermChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "entityGlossaryTermChangeEventBuilder")
  public GlossaryTermChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      Map<String, Object> parameters,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        parameters,
        auditStamp,
        semVerChange,
        description);
  }
}
