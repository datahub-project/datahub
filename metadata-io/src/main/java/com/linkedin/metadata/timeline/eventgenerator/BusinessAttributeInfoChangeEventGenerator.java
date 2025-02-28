package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

public class BusinessAttributeInfoChangeEventGenerator
    extends EntityChangeEventGenerator<BusinessAttributeInfo> {

  public static final String ATTRIBUTE_DOCUMENTATION_ADDED_FORMAT =
      "Documentation for the businessAttribute '%s' has been added: '%s'";
  public static final String ATTRIBUTE_DOCUMENTATION_REMOVED_FORMAT =
      "Documentation for the businessAttribute '%s' has been removed: '%s'";
  public static final String ATTRIBUTE_DOCUMENTATION_UPDATED_FORMAT =
      "Documentation for the businessAttribute '%s' has been updated from '%s' to '%s'.";

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<BusinessAttributeInfo> from,
      @Nonnull Aspect<BusinessAttributeInfo> to,
      @Nonnull AuditStamp auditStamp) {
    final List<ChangeEvent> changeEvents = new ArrayList<>();
    changeEvents.addAll(
        getDocumentationChangeEvent(from.getValue(), to.getValue(), urn.toString(), auditStamp));
    changeEvents.addAll(
        getGlossaryTermChangeEvents(from.getValue(), to.getValue(), urn.toString(), auditStamp));
    changeEvents.addAll(
        getTagChangeEvents(from.getValue(), to.getValue(), urn.toString(), auditStamp));
    return changeEvents;
  }

  private List<ChangeEvent> getDocumentationChangeEvent(
      BusinessAttributeInfo baseBusinessAttributeInfo,
      BusinessAttributeInfo targetBusinessAttributeInfo,
      String entityUrn,
      AuditStamp auditStamp) {
    String baseDescription =
        (baseBusinessAttributeInfo != null) ? baseBusinessAttributeInfo.getDescription() : null;
    String targetDescription =
        (targetBusinessAttributeInfo != null) ? targetBusinessAttributeInfo.getDescription() : null;
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (baseDescription == null && targetDescription != null) {
      changeEvents.add(
          createChangeEvent(
              targetBusinessAttributeInfo,
              entityUrn,
              ChangeOperation.ADD,
              ATTRIBUTE_DOCUMENTATION_ADDED_FORMAT,
              auditStamp,
              targetDescription));
    }

    if (baseDescription != null && targetDescription == null) {
      changeEvents.add(
          createChangeEvent(
              baseBusinessAttributeInfo,
              entityUrn,
              ChangeOperation.REMOVE,
              ATTRIBUTE_DOCUMENTATION_REMOVED_FORMAT,
              auditStamp,
              baseDescription));
    }

    if (baseDescription != null && !baseDescription.equals(targetDescription)) {
      changeEvents.add(
          createChangeEvent(
              targetBusinessAttributeInfo,
              entityUrn,
              ChangeOperation.MODIFY,
              ATTRIBUTE_DOCUMENTATION_UPDATED_FORMAT,
              auditStamp,
              baseDescription,
              targetDescription));
    }

    return changeEvents;
  }

  private List<ChangeEvent> getGlossaryTermChangeEvents(
      BusinessAttributeInfo baseBusinessAttributeInfo,
      BusinessAttributeInfo targetBusinessAttributeInfo,
      String entityUrn,
      AuditStamp auditStamp) {
    GlossaryTerms baseGlossaryTerms =
        (baseBusinessAttributeInfo != null) ? baseBusinessAttributeInfo.getGlossaryTerms() : null;
    GlossaryTerms targetGlossaryTerms =
        (targetBusinessAttributeInfo != null)
            ? targetBusinessAttributeInfo.getGlossaryTerms()
            : null;

    List<ChangeEvent> entityGlossaryTermsChangeEvents =
        GlossaryTermsChangeEventGenerator.computeDiffs(
            baseGlossaryTerms, targetGlossaryTerms, entityUrn.toString(), auditStamp);

    return entityGlossaryTermsChangeEvents;
  }

  private List<ChangeEvent> getTagChangeEvents(
      BusinessAttributeInfo baseBusinessAttributeInfo,
      BusinessAttributeInfo targetBusinessAttributeInfo,
      String entityUrn,
      AuditStamp auditStamp) {
    GlobalTags baseGlobalTags =
        (baseBusinessAttributeInfo != null) ? baseBusinessAttributeInfo.getGlobalTags() : null;
    GlobalTags targetGlobalTags =
        (targetBusinessAttributeInfo != null) ? targetBusinessAttributeInfo.getGlobalTags() : null;

    List<ChangeEvent> entityTagChangeEvents =
        GlobalTagsChangeEventGenerator.computeDiffs(
            baseGlobalTags, targetGlobalTags, entityUrn.toString(), auditStamp);

    return entityTagChangeEvents;
  }

  private ChangeEvent createChangeEvent(
      BusinessAttributeInfo businessAttributeInfo,
      String entityUrn,
      ChangeOperation operation,
      String format,
      AuditStamp auditStamp,
      String... descriptions) {
    List<String> args = new ArrayList<>();
    args.add(0, businessAttributeInfo.getFieldPath());
    Arrays.stream(descriptions).forEach(val -> args.add(val));
    return ChangeEvent.builder()
        .modifier(businessAttributeInfo.getFieldPath())
        .entityUrn(entityUrn)
        .category(ChangeCategory.DOCUMENTATION)
        .operation(operation)
        .semVerChange(SemanticChangeType.MINOR)
        .description(String.format(format, args.toArray()))
        .auditStamp(auditStamp)
        .build();
  }
}
