package com.linkedin.metadata.kafka.hook.incident;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentActivityChange;
import com.linkedin.incident.IncidentActivityChangeArray;
import com.linkedin.incident.IncidentActivityChangeType;
import com.linkedin.incident.IncidentActivityEvent;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * This hook is responsible for generating timeseries IncidentActivityEvent.pdl aspects when
 * incidents are updated.
 */
@Slf4j
@Component
public class IncidentActivityEventHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.RESTATE);
  private static final Set<String> SUPPORTED_UPDATE_ASPECTS =
      ImmutableSet.of(INCIDENT_INFO_ASPECT_NAME);

  private OperationContext systemOperationContext;
  private final EntityClient entityClient;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public IncidentActivityEventHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull @Value("${incidents.activityEventsHook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${incidents.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.entityClient =
        Objects.requireNonNull(systemEntityClient, "systemEntityClient is required");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public IncidentActivityEventHook(
      @Nonnull final SystemEntityClient systemEntityClient, @Nonnull Boolean isEnabled) {
    this(systemEntityClient, isEnabled, "");
  }

  @Override
  public IncidentActivityEventHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public boolean isEnabled() {
    return this.isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (this.isEnabled && isEligibleForProcessing(event)) {
      log.debug("Urn {} received by Incident Activity Event Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, systemOperationContext.getEntityRegistry());
      if (isIncidentUpdate(event)) {
        emitActivityEventForUpdate(
            urn,
            GenericRecordUtils.deserializeAspect(
                event.getPreviousAspectValue().getValue(),
                event.getPreviousAspectValue().getContentType(),
                IncidentInfo.class),
            GenericRecordUtils.deserializeAspect(
                event.getAspect().getValue(),
                event.getAspect().getContentType(),
                IncidentInfo.class));
      }
    }
  }

  /**
   * Emits an IncidentActivityEvent when a specific type of change occurs to an incident. Currently
   * supported change types: - Status is changed (ACTIVE or RESOLVED) - Lifecycle stage is changed -
   * Assignees are changed - Priority is changed
   */
  private void emitActivityEventForUpdate(
      @Nonnull final Urn incidentUrn,
      @Nonnull final IncidentInfo previousInfo,
      @Nonnull final IncidentInfo newInfo) {
    // 1. Build incident activity event
    final IncidentActivityEvent event = buildIncidentActivityEvent(previousInfo, newInfo);

    // 2. Filter non-relevant changes
    if (event == null) {
      log.debug("No incident activity event generated for incident with urn {}", incidentUrn);
      return;
    }

    // 3. Emit the event
    emitActivityEvent(incidentUrn, event);
  }

  /** Builds an IncidentActivityEvent for a given incident */
  @Nullable
  private IncidentActivityEvent buildIncidentActivityEvent(
      @Nonnull final IncidentInfo previousInfo, @Nonnull final IncidentInfo newInfo) {

    final List<IncidentActivityChange> changes = new ArrayList<>();

    // Case 1: State change
    if (previousInfo.getStatus().getState(GetMode.NULL)
        != newInfo.getStatus().getState(GetMode.NULL)) {
      changes.add(new IncidentActivityChange().setType(IncidentActivityChangeType.STATE));
    }

    // Case 2: Lifecycle stage change
    if (previousInfo.getStatus().getStage(GetMode.NULL)
        != newInfo.getStatus().getStage(GetMode.NULL)) {
      changes.add(new IncidentActivityChange().setType(IncidentActivityChangeType.STAGE));
    }

    // Case 3: Assignees change
    final List<IncidentAssignee> oldAssignees = previousInfo.getAssignees(GetMode.NULL);
    final List<IncidentAssignee> newAssignees = newInfo.getAssignees(GetMode.NULL);
    if (!areAssigneesEqual(oldAssignees, newAssignees)) {
      changes.add(new IncidentActivityChange().setType(IncidentActivityChangeType.ASSIGNEES));
    }

    // Case 4: Priority change
    if (!Objects.equals(
        previousInfo.getPriority(GetMode.NULL), newInfo.getPriority(GetMode.NULL))) {
      changes.add(new IncidentActivityChange().setType(IncidentActivityChangeType.PRIORITY));
    }

    if (changes.isEmpty()) {
      return null;
    }

    return new IncidentActivityEvent()
        .setTimestampMillis(newInfo.getStatus().getLastUpdated().getTime())
        .setActorUrn(newInfo.getStatus().getLastUpdated().getActor())
        .setChanges(new IncidentActivityChangeArray(changes))
        .setNewInfo(newInfo)
        .setPreviousInfo(previousInfo);
  }

  private boolean areAssigneesEqual(
      @Nullable final List<IncidentAssignee> oldAssignees,
      @Nullable final List<IncidentAssignee> newAssignees) {
    if (oldAssignees == null && newAssignees == null) {
      return true;
    }
    if (oldAssignees == null || newAssignees == null) {
      return false;
    }

    final Set<Urn> oldAssigneeUrns =
        oldAssignees.stream().map(IncidentAssignee::getActor).collect(Collectors.toSet());
    final Set<Urn> newAssigneeUrns =
        newAssignees.stream().map(IncidentAssignee::getActor).collect(Collectors.toSet());

    return oldAssigneeUrns.size() == newAssigneeUrns.size()
        && newAssigneeUrns.containsAll(oldAssigneeUrns);
  }

  /**
   * Returns true if the event should be processed, which is only true if the incident has been
   * updated.
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isIncidentUpdate(event);
  }

  /** Returns true if the event represents an incident update event (not a create) */
  private boolean isIncidentUpdate(@Nonnull final MetadataChangeLog event) {
    return INCIDENT_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && SUPPORTED_UPDATE_ASPECTS.contains(event.getAspectName())
        && event.hasPreviousAspectValue()
        && event.hasAspect();
  }

  /** Updates the incidents summary for a given entity */
  private void emitActivityEvent(
      @Nonnull final Urn incidentUrn, @Nonnull final IncidentActivityEvent event) {
    try {
      this.entityClient.ingestProposal(
          systemOperationContext,
          AspectUtils.buildMetadataChangeProposal(
              incidentUrn, INCIDENT_ACTIVITY_EVENT_ASPECT_NAME, event));
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to emit incident activity event for incident with urn %s! Skipping updating the summary",
              incidentUrn),
          e);
    }
  }
}
