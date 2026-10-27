package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.timeline.eventgenerator.DocumentationChangeEventGenerator.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
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

/**
 * Generates DOCUMENTATION change events from the top-level {@code description} field of a
 * properties-style aspect (e.g. {@code dataFlowInfo}, {@code editableDataJobProperties}).
 *
 * <p>Entity types whose description lives in an info/properties aspect need a generator per aspect
 * for their Change History to show description diffs. One generic generator parameterized by aspect
 * name/class replaces a near-identical hand-written class per aspect (the {@code
 * EditableDatasetPropertiesChangeEventGenerator} pattern).
 */
public class PropertiesDescriptionChangeEventGenerator<T extends RecordTemplate>
    extends EntityChangeEventGenerator<T> {

  private static final String DESCRIPTION_FIELD = "description";
  private static final String PREVIOUS_DESCRIPTION_FIELD = "previousDescription";

  private final String aspectName;
  private final Class<T> aspectClass;
  private final String descriptionFieldName;

  /**
   * @param descriptionFieldName the top-level string field holding the free-text description;
   *     usually {@code description}, but e.g. {@code definition} for {@code glossaryNodeInfo}.
   */
  public PropertiesDescriptionChangeEventGenerator(
      @Nonnull final String aspectName,
      @Nonnull final Class<T> aspectClass,
      @Nonnull final String descriptionFieldName) {
    this.aspectName = aspectName;
    this.aspectClass = aspectClass;
    this.descriptionFieldName = descriptionFieldName;
  }

  @Nullable
  private String getDescription(@Nullable final RecordTemplate properties) {
    if (properties == null) {
      return null;
    }
    return (String) properties.data().get(descriptionFieldName);
  }

  private List<ChangeEvent> computeDiffs(
      @Nullable final RecordTemplate baseProperties,
      @Nullable final RecordTemplate targetProperties,
      @Nonnull final String entityUrn,
      @Nullable final AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    String baseDescription = getDescription(baseProperties);
    String targetDescription = getDescription(targetProperties);

    if (baseDescription == null && targetDescription != null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_ADDED, entityUrn, targetDescription))
              .parameters(ImmutableMap.of(DESCRIPTION_FIELD, targetDescription))
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
              .parameters(ImmutableMap.of(DESCRIPTION_FIELD, baseDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null && !baseDescription.equals(targetDescription)) {
      // The UI diff view (DocumentationDiff) requires previousDescription on MODIFY events —
      // without it the change row renders empty.
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(DESCRIPTION_CHANGED, entityUrn, baseDescription, targetDescription))
              .parameters(
                  ImmutableMap.of(
                      DESCRIPTION_FIELD,
                      targetDescription,
                      PREVIOUS_DESCRIPTION_FIELD,
                      baseDescription))
              .auditStamp(auditStamp)
              .build());
    }
    return changeEvents;
  }

  @Nullable
  private T getPropertiesFromAspect(@Nullable final EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(aspectClass, entityAspect.getMetadata());
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
    if (!previousValue.getAspect().equals(aspectName)
        || !currentValue.getAspect().equals(aspectName)) {
      throw new IllegalArgumentException("Aspect is not " + aspectName);
    }
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      changeEvents.addAll(
          computeDiffs(
              getPropertiesFromAspect(previousValue),
              getPropertiesFromAspect(currentValue),
              currentValue.getUrn(),
              null));
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
      @Nonnull Aspect<T> from,
      @Nonnull Aspect<T> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
