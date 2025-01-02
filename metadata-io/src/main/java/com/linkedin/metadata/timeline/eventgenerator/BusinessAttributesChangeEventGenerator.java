package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldBusinessAttributeChangeEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BusinessAttributesChangeEventGenerator
    extends EntityChangeEventGenerator<BusinessAttributes> {

  private static final String BUSINESS_ATTRIBUTE_ADDED_FORMAT =
      "BusinessAttribute '%s' added to entity '%s'.";
  private static final String BUSINESS_ATTRIBUTE_REMOVED_FORMAT =
      "BusinessAttribute '%s' removed from entity '%s'.";

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Aspect<BusinessAttributes> from,
      @Nonnull Aspect<BusinessAttributes> to,
      @Nonnull AuditStamp auditStamp) {
    log.debug(
        "Calling BusinessAttributesChangeEventGenerator for entity {} and aspect {}",
        entityName,
        aspectName);
    return computeDiff(urn, entityName, aspectName, from.getValue(), to.getValue(), auditStamp);
  }

  private List<ChangeEvent> computeDiff(
      Urn urn,
      String entityName,
      String aspectName,
      BusinessAttributes previousValue,
      BusinessAttributes newValue,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    BusinessAttributeAssociation previousAssociation =
        previousValue != null ? previousValue.getBusinessAttribute() : null;
    BusinessAttributeAssociation newAssociation =
        newValue != null ? newValue.getBusinessAttribute() : null;

    if (Objects.nonNull(previousAssociation) && Objects.isNull(newAssociation)) {
      changeEvents.add(
          createChangeEvent(
              previousAssociation,
              urn,
              ChangeOperation.REMOVE,
              BUSINESS_ATTRIBUTE_REMOVED_FORMAT,
              auditStamp));

    } else if (Objects.isNull(previousAssociation) && Objects.nonNull(newAssociation)) {
      changeEvents.add(
          createChangeEvent(
              newAssociation,
              urn,
              ChangeOperation.ADD,
              BUSINESS_ATTRIBUTE_ADDED_FORMAT,
              auditStamp));
    }
    return changeEvents;
  }

  private ChangeEvent createChangeEvent(
      BusinessAttributeAssociation businessAttributeAssociation,
      Urn entityUrn,
      ChangeOperation changeOperation,
      String format,
      AuditStamp auditStamp) {
    return SchemaFieldBusinessAttributeChangeEvent.schemaFieldBusinessAttributeChangeEventBuilder()
        .entityUrn(entityUrn.toString())
        .category(ChangeCategory.BUSINESS_ATTRIBUTE)
        .operation(changeOperation)
        .modifier(businessAttributeAssociation.getBusinessAttributeUrn().toString())
        .auditStamp(auditStamp)
        .semVerChange(SemanticChangeType.MINOR)
        .description(
            String.format(
                format, businessAttributeAssociation.getBusinessAttributeUrn().getId(), entityUrn))
        .parentUrn(entityUrn)
        .businessAttributeUrn(businessAttributeAssociation.getBusinessAttributeUrn())
        .datasetUrn(entityUrn.getIdAsUrn())
        .build();
  }
}
