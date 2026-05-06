package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.DomainProperties;
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

public class DomainPropertiesChangeEventGenerator
    extends EntityChangeEventGenerator<DomainProperties> {
  private static final String NAME_ADDED_FORMAT = "Name for '%s' has been set: '%s'.";
  private static final String NAME_CHANGED_FORMAT =
      "Name of '%s' has been changed from '%s' to '%s'.";
  private static final String DESCRIPTION_ADDED_FORMAT =
      "Description for '%s' has been added: '%s'.";
  private static final String DESCRIPTION_REMOVED_FORMAT =
      "Description for '%s' has been removed: '%s'.";
  private static final String DESCRIPTION_CHANGED_FORMAT =
      "Description of '%s' has been changed from '%s' to '%s'.";

  private static List<ChangeEvent> computeDiffs(
      @Nullable DomainProperties baseDomainProperties,
      @Nonnull DomainProperties targetDomainProperties,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    // Diff name (required field, so target always has it)
    String baseName = (baseDomainProperties != null) ? baseDomainProperties.getName() : null;
    String targetName = targetDomainProperties.getName();

    if (baseName == null && targetName != null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(NAME_ADDED_FORMAT, entityUrn, targetName))
              .auditStamp(auditStamp)
              .build());
    } else if (baseName != null && targetName != null && !baseName.equals(targetName)) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(NAME_CHANGED_FORMAT, entityUrn, baseName, targetName))
              .auditStamp(auditStamp)
              .build());
    }

    // Diff description (optional field)
    String baseDescription =
        (baseDomainProperties != null) ? baseDomainProperties.getDescription() : null;
    String targetDescription = targetDomainProperties.getDescription();

    if (baseDescription == null && targetDescription != null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_ADDED_FORMAT, entityUrn, targetDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null && targetDescription == null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_REMOVED_FORMAT, entityUrn, baseDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null
        && targetDescription != null
        && !baseDescription.equals(targetDescription)) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      DESCRIPTION_CHANGED_FORMAT, entityUrn, baseDescription, targetDescription))
              .auditStamp(auditStamp)
              .build());
    }

    return changeEvents;
  }

  @Nullable
  private static DomainProperties getDomainPropertiesFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(DomainProperties.class, entityAspect.getMetadata());
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
    if (!previousValue.getAspect().equals(DOMAIN_PROPERTIES_ASPECT_NAME)
        || !currentValue.getAspect().equals(DOMAIN_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + DOMAIN_PROPERTIES_ASPECT_NAME);
    }
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      DomainProperties baseDomainProperties = getDomainPropertiesFromAspect(previousValue);
      DomainProperties targetDomainProperties = getDomainPropertiesFromAspect(currentValue);
      if (targetDomainProperties != null) {
        changeEvents.addAll(
            computeDiffs(
                baseDomainProperties, targetDomainProperties, currentValue.getUrn(), null));
      }
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
      @Nonnull Aspect<DomainProperties> from,
      @Nonnull Aspect<DomainProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
