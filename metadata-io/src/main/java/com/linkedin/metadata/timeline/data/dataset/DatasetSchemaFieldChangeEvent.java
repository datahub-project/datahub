package com.linkedin.metadata.timeline.data.dataset;

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
public class DatasetSchemaFieldChangeEvent extends ChangeEvent {
  /**
   * The field path that was involved in the change event.
   */
  String fieldPath;

  /**
   * The field urn that was involved in the change event.
   */
  Urn fieldUrn;

  /**
   * Whether the field is nullable
   */
  boolean nullable;

  @Builder(builderMethodName = "schemaFieldChangeEventBuilder")
  public DatasetSchemaFieldChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      String fieldPath,
      Urn fieldUrn,
      boolean nullable
  ) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of(
            "fieldPath", fieldPath,
            "fieldUrn", fieldUrn.toString(),
            "nullable", nullable
        ),
        auditStamp,
        semVerChange,
        description
    );
    this.fieldPath = fieldPath;
    this.fieldUrn = fieldUrn;
    this.nullable = nullable;
  }
}

