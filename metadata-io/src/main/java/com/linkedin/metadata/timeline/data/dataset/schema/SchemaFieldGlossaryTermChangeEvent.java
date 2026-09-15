package com.linkedin.metadata.timeline.data.dataset.schema;

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

@EqualsAndHashCode(callSuper = true)
@Value
@Getter
public class SchemaFieldGlossaryTermChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "schemaFieldGlossaryTermChangeEventBuilder")
  public SchemaFieldGlossaryTermChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      Map<String, Object> parameters,
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
