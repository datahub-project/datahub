package com.linkedin.metadata.timeline.eventgenerator;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Generates change events for the Status aspect's lifecycleStage field, tracking lifecycle stage
 * transitions (e.g. Draft -> Published). Can be used for any entity type that has the Status
 * aspect.
 */
@Slf4j
public class LifecycleStageChangeEventGenerator extends EntityChangeEventGenerator<Status> {

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<Status> from,
      @Nonnull Aspect<Status> to,
      @Nonnull AuditStamp auditStamp) {
    return new ArrayList<>();
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      @Nullable EntityAspect previousValue,
      @Nonnull EntityAspect currentValue,
      @Nonnull ChangeCategory changeCategory,
      @Nullable JsonPatch rawDiff,
      boolean rawDiffsRequested) {

    List<ChangeEvent> changeEvents = new ArrayList<>();
    AuditStamp auditStamp = getAuditStamp(currentValue);
    String entityUrn = currentValue.getUrn();

    if (changeCategory != ChangeCategory.LIFECYCLE) {
      return buildTransaction(changeEvents, currentValue);
    }

    Status oldStatus = previousValue != null ? getStatusFromAspect(previousValue) : null;
    Status newStatus = getStatusFromAspect(currentValue);

    if (newStatus == null) {
      return buildTransaction(changeEvents, currentValue);
    }

    String oldStage =
        oldStatus != null && oldStatus.hasLifecycleStage()
            ? getStageLabel(oldStatus.getLifecycleStage())
            : null;
    String newStage =
        newStatus.hasLifecycleStage() ? getStageLabel(newStatus.getLifecycleStage()) : null;

    if (!Objects.equals(oldStage, newStage)) {
      String description;
      if (oldStage == null && newStage != null) {
        description = String.format("Lifecycle state changed to %s", newStage);
      } else if (oldStage != null && newStage == null) {
        description = String.format("Lifecycle state %s was removed", oldStage);
      } else {
        description = String.format("Lifecycle state changed from %s to %s", oldStage, newStage);
      }

      changeEvents.add(
          ChangeEvent.builder()
              .category(ChangeCategory.LIFECYCLE)
              .operation(ChangeOperation.MODIFY)
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .description(description)
              .parameters(
                  Map.of(
                      "oldState", oldStage != null ? oldStage : "",
                      "newState", newStage != null ? newStage : ""))
              .build());
    }

    return buildTransaction(changeEvents, currentValue);
  }

  /**
   * Extracts the human-readable stage label from a lifecycle stage URN. For example,
   * "urn:li:lifecycleStageType:PUBLISHED" returns "published".
   */
  private String getStageLabel(@Nonnull Urn lifecycleStageUrn) {
    return lifecycleStageUrn.getId().toLowerCase();
  }

  private ChangeTransaction buildTransaction(List<ChangeEvent> events, EntityAspect currentValue) {
    return ChangeTransaction.builder()
        .changeEvents(events)
        .timestamp(currentValue.getCreatedOn().getTime())
        .semVerChange(SemanticChangeType.NONE)
        .build();
  }

  @Nullable
  private Status getStatusFromAspect(@Nonnull EntityAspect aspect) {
    try {
      return RecordUtils.toRecordTemplate(Status.class, aspect.getMetadata());
    } catch (Exception e) {
      log.error("Failed to deserialize Status from aspect", e);
      return null;
    }
  }

  private AuditStamp getAuditStamp(@Nonnull EntityAspect aspect) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());
    try {
      auditStamp.setActor(Urn.createFromString(aspect.getCreatedBy()));
    } catch (Exception e) {
      log.warn("Failed to parse actor URN: {}", aspect.getCreatedBy());
    }
    return auditStamp;
  }
}
