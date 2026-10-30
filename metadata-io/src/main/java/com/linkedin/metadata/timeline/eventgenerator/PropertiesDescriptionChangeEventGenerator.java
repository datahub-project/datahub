package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.timeline.eventgenerator.DocumentationChangeEventGenerator.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;

/**
 * Generates change events from a declared list of top-level fields of a properties-style aspect
 * (e.g. {@code dataFlowInfo.description}, {@code glossaryNodeInfo.parentNode}).
 *
 * <p>One generic generator parameterized by aspect name/class and tracked fields replaces a
 * near-identical hand-written class per aspect (the {@code
 * EditableDatasetPropertiesChangeEventGenerator} pattern). Fields of the aspect that are neither
 * tracked nor listed in {@code ignoredFields} (with a reason) fail {@code
 * TimelineCoverageGuardTest}.
 */
public class PropertiesDescriptionChangeEventGenerator<T extends RecordTemplate>
    extends EntityChangeEventGenerator<T> {

  private static final String DESCRIPTION_PARAM = "description";
  private static final String PREVIOUS_DESCRIPTION_PARAM = "previousDescription";
  private static final String FIELD_PARAM = "field";
  private static final String KEY_PARAM = "key";
  private static final String VALUE_PARAM = "value";
  private static final String PREVIOUS_VALUE_PARAM = "previousValue";
  private static final String OLD_PARENT_PARAM = "oldParent";
  private static final String NEW_PARENT_PARAM = "newParent";

  public enum FieldKind {
    /** Free text rendered by the UI documentation diff; params {@code description}. */
    DESCRIPTION,
    /** Any other string field; params {@code field}, {@code value}. */
    TEXT,
    /** Parent URN field; PARENT events with {@code oldParent} / {@code newParent}. */
    PARENT,
    /** String-to-string map, diffed per key; params {@code field}, {@code key}, {@code value}. */
    STRING_MAP
  }

  /**
   * One tracked field. Formats take {@code (entityUrn, value)} for added/removed and {@code
   * (entityUrn, previous, value)} for changed; STRING_MAP inserts the key as the second argument.
   */
  @Value
  public static class TrackedField {
    String fieldName;
    ChangeCategory category;
    FieldKind kind;
    String addedFormat;
    String removedFormat;
    String changedFormat;

    public static TrackedField description(@Nonnull final String fieldName) {
      return new TrackedField(
          fieldName,
          ChangeCategory.DOCUMENTATION,
          FieldKind.DESCRIPTION,
          DESCRIPTION_ADDED,
          DESCRIPTION_REMOVED,
          DESCRIPTION_CHANGED);
    }

    public static TrackedField text(@Nonnull final String fieldName, @Nonnull final String label) {
      return new TrackedField(
          fieldName,
          ChangeCategory.DOCUMENTATION,
          FieldKind.TEXT,
          label + " for '%s' has been set: '%s'.",
          label + " for '%s' has been removed: '%s'.",
          label + " of '%s' has been changed from '%s' to '%s'.");
    }

    public static TrackedField parent(@Nonnull final String fieldName) {
      return new TrackedField(
          fieldName,
          ChangeCategory.PARENT,
          FieldKind.PARENT,
          "Moved '%s' to '%s'.",
          "Moved '%s' to the top level from '%s'.",
          "Moved '%s' from '%s' to '%s'.");
    }

    public static TrackedField stringMap(
        @Nonnull final String fieldName, @Nonnull final String label) {
      return new TrackedField(
          fieldName,
          ChangeCategory.DOCUMENTATION,
          FieldKind.STRING_MAP,
          label + " '%2$s' for '%1$s' has been set: '%3$s'.",
          label + " '%2$s' for '%1$s' has been removed: '%3$s'.",
          label + " '%2$s' of '%1$s' has been changed from '%3$s' to '%4$s'.");
    }
  }

  private final String aspectName;
  private final Class<T> aspectClass;
  private final List<TrackedField> trackedFields;

  /** Aspect fields deliberately not tracked, mapped to the reason. */
  @Getter private final Map<String, String> ignoredFields;

  public PropertiesDescriptionChangeEventGenerator(
      @Nonnull final String aspectName,
      @Nonnull final Class<T> aspectClass,
      @Nonnull final List<TrackedField> trackedFields,
      @Nonnull final Map<String, String> ignoredFields) {
    this.aspectName = aspectName;
    this.aspectClass = aspectClass;
    this.trackedFields = List.copyOf(trackedFields);
    this.ignoredFields = Map.copyOf(ignoredFields);
  }

  public Set<String> getTrackedFieldNames() {
    return trackedFields.stream().map(TrackedField::getFieldName).collect(Collectors.toSet());
  }

  @Nullable
  private static Object getField(
      @Nullable final RecordTemplate properties, @Nonnull final String fieldName) {
    return properties == null ? null : properties.data().get(fieldName);
  }

  private List<ChangeEvent> computeDiffs(
      @Nullable final RecordTemplate baseProperties,
      @Nullable final RecordTemplate targetProperties,
      @Nonnull final String entityUrn,
      @Nullable final AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    for (TrackedField field : trackedFields) {
      // No previous aspect (timeline sentinel or first write) means creation, not a move.
      if (field.getKind() == FieldKind.PARENT && baseProperties == null) {
        continue;
      }
      Object base = getField(baseProperties, field.getFieldName());
      Object target = getField(targetProperties, field.getFieldName());
      if (field.getKind() == FieldKind.STRING_MAP) {
        diffStringMap(field, (DataMap) base, (DataMap) target, entityUrn, auditStamp, changeEvents);
      } else {
        diffString(field, (String) base, (String) target, entityUrn, auditStamp, changeEvents);
      }
    }
    return changeEvents;
  }

  private static void diffString(
      @Nonnull final TrackedField field,
      @Nullable final String base,
      @Nullable final String target,
      @Nonnull final String entityUrn,
      @Nullable final AuditStamp auditStamp,
      @Nonnull final List<ChangeEvent> changeEvents) {
    if (Objects.equals(base, target)) {
      return;
    }
    final ChangeOperation operation;
    final String description;
    if (base == null) {
      operation = ChangeOperation.ADD;
      description = String.format(field.getAddedFormat(), entityUrn, target);
    } else if (target == null) {
      operation = ChangeOperation.REMOVE;
      description = String.format(field.getRemovedFormat(), entityUrn, base);
    } else {
      operation = ChangeOperation.MODIFY;
      description = String.format(field.getChangedFormat(), entityUrn, base, target);
    }

    Map<String, Object> params = new HashMap<>();
    switch (field.getKind()) {
      case DESCRIPTION:
        // The UI diff view (DocumentationDiff) requires previousDescription on MODIFY events —
        // without it the change row renders empty.
        params.put(DESCRIPTION_PARAM, target != null ? target : base);
        if (operation == ChangeOperation.MODIFY) {
          params.put(PREVIOUS_DESCRIPTION_PARAM, base);
        }
        break;
      case PARENT:
        if (base != null) {
          params.put(OLD_PARENT_PARAM, base);
        }
        if (target != null) {
          params.put(NEW_PARENT_PARAM, target);
        }
        break;
      default:
        params.put(FIELD_PARAM, field.getFieldName());
        params.put(VALUE_PARAM, target != null ? target : base);
        if (operation == ChangeOperation.MODIFY) {
          params.put(PREVIOUS_VALUE_PARAM, base);
        }
    }

    changeEvents.add(
        ChangeEvent.builder()
            .entityUrn(entityUrn)
            .category(field.getCategory())
            // A move is a change of the one parent slot, matching DocumentInfoChangeEventGenerator.
            .operation(field.getKind() == FieldKind.PARENT ? ChangeOperation.MODIFY : operation)
            .semVerChange(SemanticChangeType.MINOR)
            .description(description)
            .parameters(params)
            .auditStamp(auditStamp)
            .build());
  }

  private static void diffStringMap(
      @Nonnull final TrackedField field,
      @Nullable final DataMap base,
      @Nullable final DataMap target,
      @Nonnull final String entityUrn,
      @Nullable final AuditStamp auditStamp,
      @Nonnull final List<ChangeEvent> changeEvents) {
    Set<String> keys = new TreeSet<>();
    if (base != null) {
      keys.addAll(base.keySet());
    }
    if (target != null) {
      keys.addAll(target.keySet());
    }
    for (String key : keys) {
      String before = base == null ? null : (String) base.get(key);
      String after = target == null ? null : (String) target.get(key);
      if (Objects.equals(before, after)) {
        continue;
      }
      Map<String, Object> params = new HashMap<>();
      params.put(FIELD_PARAM, field.getFieldName());
      params.put(KEY_PARAM, key);
      params.put(VALUE_PARAM, after != null ? after : before);
      final ChangeOperation operation;
      final String description;
      if (before == null) {
        operation = ChangeOperation.ADD;
        description = String.format(field.getAddedFormat(), entityUrn, key, after);
      } else if (after == null) {
        operation = ChangeOperation.REMOVE;
        description = String.format(field.getRemovedFormat(), entityUrn, key, before);
      } else {
        operation = ChangeOperation.MODIFY;
        description = String.format(field.getChangedFormat(), entityUrn, key, before, after);
        params.put(PREVIOUS_VALUE_PARAM, before);
      }
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(field.getCategory())
              .operation(operation)
              .semVerChange(SemanticChangeType.MINOR)
              .description(description)
              .parameters(params)
              .auditStamp(auditStamp)
              .build());
    }
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
    List<ChangeEvent> changeEvents =
        computeDiffs(
                getPropertiesFromAspect(previousValue),
                getPropertiesFromAspect(currentValue),
                currentValue.getUrn(),
                null)
            .stream()
            .filter(event -> event.getCategory() == element)
            .collect(Collectors.toList());

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
