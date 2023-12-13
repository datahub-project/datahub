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
import lombok.Getter;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
@Getter
public class DatasetSchemaFieldChangeEvent extends ChangeEvent {
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
<<<<<<< HEAD
      boolean nullable,
      SchemaFieldModificationCategory modificationCategory) {
=======
      boolean nullable) {
>>>>>>> oss_master
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of(
<<<<<<< HEAD
            "fieldPath",
            fieldPath,
            "fieldUrn",
            fieldUrn.toString(),
            "nullable",
            nullable,
            "modificationCategory",
            modificationCategory != null
                ? modificationCategory.toString()
                : SchemaFieldModificationCategory.OTHER.toString()),
=======
            "fieldPath", fieldPath,
            "fieldUrn", fieldUrn.toString(),
            "nullable", nullable),
>>>>>>> oss_master
        auditStamp,
        semVerChange,
        description);
  }
}
