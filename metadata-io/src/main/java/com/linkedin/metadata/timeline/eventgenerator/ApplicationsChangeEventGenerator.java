package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.application.Applications;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ApplicationsChangeEventGenerator extends EntityChangeEventGenerator<Applications> {
  private static final String APPLICATION_ADDED_FORMAT = "Application '%s' added to entity '%s'.";
  private static final String APPLICATION_REMOVED_FORMAT =
      "Application '%s' removed from entity '%s'.";

  private static List<ChangeEvent> computeDiffs(
      @Nullable Applications baseApplications,
      @Nullable Applications targetApplications,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {

    List<Urn> base =
        baseApplications != null
            ? new ArrayList<>(baseApplications.getApplications())
            : new ArrayList<>();
    List<Urn> target =
        targetApplications != null
            ? new ArrayList<>(targetApplications.getApplications())
            : new ArrayList<>();

    base.sort(Comparator.comparing(Urn::toString));
    target.sort(Comparator.comparing(Urn::toString));

    List<ChangeEvent> changeEvents = new ArrayList<>();
    int baseIdx = 0;
    int targetIdx = 0;

    while (baseIdx < base.size() && targetIdx < target.size()) {
      int comparison = base.get(baseIdx).toString().compareTo(target.get(targetIdx).toString());
      if (comparison == 0) {
        ++baseIdx;
        ++targetIdx;
      } else if (comparison < 0) {
        changeEvents.add(
            buildEvent(base.get(baseIdx), entityUrn, ChangeOperation.REMOVE, auditStamp));
        ++baseIdx;
      } else {
        changeEvents.add(
            buildEvent(target.get(targetIdx), entityUrn, ChangeOperation.ADD, auditStamp));
        ++targetIdx;
      }
    }

    while (baseIdx < base.size()) {
      changeEvents.add(
          buildEvent(base.get(baseIdx), entityUrn, ChangeOperation.REMOVE, auditStamp));
      ++baseIdx;
    }
    while (targetIdx < target.size()) {
      changeEvents.add(
          buildEvent(target.get(targetIdx), entityUrn, ChangeOperation.ADD, auditStamp));
      ++targetIdx;
    }

    return changeEvents;
  }

  private static ChangeEvent buildEvent(
      Urn applicationUrn, String entityUrn, ChangeOperation operation, AuditStamp auditStamp) {
    String format =
        (operation == ChangeOperation.ADD) ? APPLICATION_ADDED_FORMAT : APPLICATION_REMOVED_FORMAT;
    return ChangeEvent.builder()
        .modifier(applicationUrn.toString())
        .entityUrn(entityUrn)
        .category(ChangeCategory.APPLICATION)
        .operation(operation)
        .semVerChange(SemanticChangeType.MINOR)
        .description(String.format(format, applicationUrn.getId(), entityUrn))
        .auditStamp(auditStamp)
        .build();
  }

  @Nullable
  private static Applications getApplicationsFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(Applications.class, entityAspect.getMetadata());
    }
    return null;
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(APPLICATION_MEMBERSHIP_ASPECT_NAME)
        || !currentValue.getAspect().equals(APPLICATION_MEMBERSHIP_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + APPLICATION_MEMBERSHIP_ASPECT_NAME);
    }

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.APPLICATION) {
      Applications baseApplications = getApplicationsFromAspect(previousValue);
      Applications targetApplications = getApplicationsFromAspect(currentValue);
      changeEvents.addAll(
          computeDiffs(baseApplications, targetApplications, currentValue.getUrn(), null));
    }

    SemanticChangeType highestSemanticChange = SemanticChangeType.NONE;
    ChangeEvent highestChangeEvent =
        changeEvents.stream().max(Comparator.comparing(ChangeEvent::getSemVerChange)).orElse(null);
    if (highestChangeEvent != null) {
      highestSemanticChange = highestChangeEvent.getSemVerChange();
    }

    return ChangeTransaction.builder()
        .semVerChange(highestSemanticChange)
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffsRequested ? rawDiff : null)
        .actor(currentValue.getCreatedBy())
        .build();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<Applications> from,
      @Nonnull Aspect<Applications> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
