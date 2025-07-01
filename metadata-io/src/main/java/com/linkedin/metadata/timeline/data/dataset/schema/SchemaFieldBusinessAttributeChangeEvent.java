package com.linkedin.metadata.timeline.data.dataset.schema;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import lombok.Builder;

public class SchemaFieldBusinessAttributeChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "schemaFieldBusinessAttributeChangeEventBuilder")
  public SchemaFieldBusinessAttributeChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      Urn parentUrn,
      Urn businessAttributeUrn,
      Urn datasetUrn) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of(
            "parentUrn", parentUrn.toString(),
            "businessAttributeUrn", businessAttributeUrn.toString(),
            "datasetUrn", datasetUrn.toString()),
        auditStamp,
        semVerChange,
        description);
  }
}
