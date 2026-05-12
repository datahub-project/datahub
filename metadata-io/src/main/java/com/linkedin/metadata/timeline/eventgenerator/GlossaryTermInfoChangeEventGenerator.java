package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.timeline.eventgenerator.DocumentationChangeEventGenerator.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.glossary.GlossaryTermInfo;
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

public class GlossaryTermInfoChangeEventGenerator
    extends EntityChangeEventGenerator<GlossaryTermInfo> {
  private static final String NAME_ADDED_FORMAT = "Name for '%s' has been set: '%s'.";
  private static final String NAME_CHANGED_FORMAT =
      "Name of '%s' has been changed from '%s' to '%s'.";

  private static List<ChangeEvent> computeDiffs(
      GlossaryTermInfo baseTermInfo,
      @Nonnull GlossaryTermInfo targetTermInfo,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    // Diff name (optional field)
    String baseName = (baseTermInfo != null) ? baseTermInfo.getName() : null;
    String targetName = targetTermInfo.getName();

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

    // Diff description (definition field)
    String baseDescription = (baseTermInfo != null) ? baseTermInfo.getDefinition() : null;
    String targetDescription = (targetTermInfo != null) ? targetTermInfo.getDefinition() : null;

    if (baseDescription == null && targetDescription != null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_ADDED, entityUrn, targetDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null && targetDescription == null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_REMOVED, entityUrn, baseDescription))
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
                  String.format(DESCRIPTION_CHANGED, entityUrn, baseDescription, targetDescription))
              .auditStamp(auditStamp)
              .build());
    }
    return changeEvents;
  }

  @Nullable
  private static GlossaryTermInfo getGlossaryTermInfoFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(GlossaryTermInfo.class, entityAspect.getMetadata());
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
    if (!previousValue.getAspect().equals(GLOSSARY_TERM_INFO_ASPECT_NAME)
        || !currentValue.getAspect().equals(GLOSSARY_TERM_INFO_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + GLOSSARY_TERM_INFO_ASPECT_NAME);
    }
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      GlossaryTermInfo baseGlossaryTermInfo = getGlossaryTermInfoFromAspect(previousValue);
      GlossaryTermInfo targetGlossaryTermInfo = getGlossaryTermInfoFromAspect(currentValue);
      changeEvents.addAll(
          computeDiffs(baseGlossaryTermInfo, targetGlossaryTermInfo, currentValue.getUrn(), null));
    }

    // Assess the highest change at the transaction(schema) level.
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
      @Nonnull Aspect<GlossaryTermInfo> from,
      @Nonnull Aspect<GlossaryTermInfo> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
