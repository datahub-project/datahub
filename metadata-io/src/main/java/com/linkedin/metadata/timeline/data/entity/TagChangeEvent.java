package com.linkedin.metadata.timeline.data.entity;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.TagAssociation;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@Getter
public class TagChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "entityTagChangeEventBuilder")
  public TagChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      TagAssociation tagAssociation) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        buildParameters(tagAssociation),
        auditStamp,
        semVerChange,
        description);
  }

  private static ImmutableMap<String, Object> buildParameters(TagAssociation tagAssociation) {
    return new ImmutableMap.Builder<String, Object>()
        .put("tagUrn", tagAssociation.getTag().toString())
        .put("context", tagAssociation.getContext() != null ? tagAssociation.getContext() : "{}")
        .put(
            "sourceDetails",
            ChangeEventParameterUtils.serializeSourceDetail(tagAssociation.getAttribution()))
        .build();
  }
}
