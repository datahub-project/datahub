package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.BusinessAttributeAssociationChangeEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

public class BusinessAttributeAssociationChangeEventGenerator
    extends EntityChangeEventGenerator<BusinessAttributeAssociation> {

  private static final String BUSINESS_ATTRIBUTE_ADDED_FORMAT =
      "BusinessAttribute '%s' added to entity '%s'.";
  private static final String BUSINESS_ATTRIBUTE_REMOVED_FORMAT =
      "BusinessAttribute '%s' removed from entity '%s'.";

  public static List<ChangeEvent> computeDiffs(
      BusinessAttributeAssociation baseAssociation,
      BusinessAttributeAssociation targetAssociation,
      String urn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    if (Objects.nonNull(baseAssociation) && Objects.isNull(targetAssociation)) {
      changeEvents.add(
          createChangeEvent(
              baseAssociation,
              urn,
              ChangeOperation.REMOVE,
              BUSINESS_ATTRIBUTE_REMOVED_FORMAT,
              auditStamp));

    } else if (Objects.isNull(baseAssociation) && Objects.nonNull(targetAssociation)) {
      changeEvents.add(
          createChangeEvent(
              targetAssociation,
              urn,
              ChangeOperation.ADD,
              BUSINESS_ATTRIBUTE_ADDED_FORMAT,
              auditStamp));
    }
    return changeEvents;
  }

  private static ChangeEvent createChangeEvent(
      BusinessAttributeAssociation association,
      String entityUrn,
      ChangeOperation operation,
      String format,
      AuditStamp auditStamp) {
    return BusinessAttributeAssociationChangeEvent
        .entityBusinessAttributeAssociationChangeEventBuilder()
        .modifier(association.getBusinessAttributeUrn().toString())
        .entityUrn(entityUrn)
        .category(ChangeCategory.BUSINESS_ATTRIBUTE)
        .operation(operation)
        .semVerChange(SemanticChangeType.MINOR)
        .description(
            String.format(format, association.getBusinessAttributeUrn().getId(), entityUrn))
        .businessAttributeUrn(association.getBusinessAttributeUrn())
        .auditStamp(auditStamp)
        .build();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<BusinessAttributeAssociation> from,
      @Nonnull Aspect<BusinessAttributeAssociation> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
