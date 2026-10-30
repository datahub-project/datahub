package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.DEPRECATION_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import jakarta.json.JsonPatch;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Differ responsible for determining whether an entity has been deprecated or un-deprecated. */
public class DeprecationChangeEventGenerator extends EntityChangeEventGenerator<Deprecation> {

  private static final String DEPRECATED_FORMAT = "'%s' has been deprecated.";
  private static final String UNDEPRECATED_FORMAT = "'%s' is no longer deprecated.";

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {
    if (!currentValue.getAspect().equals(DEPRECATION_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + DEPRECATION_ASPECT_NAME);
    }
    List<ChangeEvent> changeEvents =
        element == ChangeCategory.DEPRECATION
            ? computeDiffs(
                getDeprecationFromAspect(previousValue),
                getDeprecationFromAspect(currentValue),
                currentValue.getUrn(),
                null)
            : Collections.emptyList();
    return ChangeTransaction.builder()
        .semVerChange(changeEvents.isEmpty() ? SemanticChangeType.NONE : SemanticChangeType.MINOR)
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffsRequested ? rawDiff : null)
        .actor(currentValue.getCreatedBy())
        .build();
  }

  @Nullable
  private static Deprecation getDeprecationFromAspect(@Nullable final EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(Deprecation.class, entityAspect.getMetadata());
    }
    return null;
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<Deprecation> from,
      @Nonnull Aspect<Deprecation> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      Deprecation baseDeprecation,
      Deprecation targetDeprecation,
      String entityUrn,
      AuditStamp auditStamp) {

    // Ensure that it is the deprecation status which has actually been changed.

    // If the entity was not previously deprecated, but is now deprecated, then return a deprecated
    // event.
    if (!isDeprecated(baseDeprecation) && isDeprecated(targetDeprecation)) {
      final Map<String, Object> parametersMap = buildDeprecatedParameters(targetDeprecation);
      return Collections.singletonList(
          ChangeEvent.builder()
              .category(ChangeCategory.DEPRECATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DEPRECATED_FORMAT, entityUrn))
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .parameters(parametersMap)
              .build());
    }

    // If the entity was previously deprecated, but is not not deprecated, then return a
    // un-deprecated event.
    if (isDeprecated(baseDeprecation) && !isDeprecated(targetDeprecation)) {
      return Collections.singletonList(
          ChangeEvent.builder()
              .category(ChangeCategory.DEPRECATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(UNDEPRECATED_FORMAT, entityUrn))
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .parameters(ImmutableMap.of("status", "ACTIVE"))
              .build());
    }

    return Collections.emptyList();
  }

  private boolean isDeprecated(@Nullable final Deprecation deprecation) {
    return deprecation != null && deprecation.isDeprecated();
  }

  private Map<String, Object> buildDeprecatedParameters(Deprecation deprecation) {
    Map<String, Object> parametersMap = new HashMap<>();
    parametersMap.put("status", "DEPRECATED");
    if (deprecation.hasDecommissionTime()) {
      parametersMap.put("timestamp", deprecation.getDecommissionTime().toString());
    }
    if (deprecation.hasNote()) {
      parametersMap.put("note", deprecation.getNote());
    }
    return parametersMap;
  }
}
