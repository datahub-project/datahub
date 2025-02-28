package com.linkedin.metadata.boot.steps;

import com.linkedin.common.IncidentSummaryDetails;
import com.linkedin.common.IncidentSummaryDetailsArray;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.service.IncidentService;
import com.linkedin.metadata.service.IncidentsSummaryUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Upgrade step that migrates existing Incident Summary aspects to use the newer
 * activeIncidentDetails and resolvedIncidentDetails fields.
 */
@Slf4j
public class MigrateIncidentsSummaryStep extends UpgradeStep {
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "migrate-incidents-summary";
  private static final Integer BATCH_SIZE = 1000;

  private final EntitySearchService _entitySearchService;
  private final IncidentService _incidentService;

  public MigrateIncidentsSummaryStep(
      EntityService<?> entityService,
      EntitySearchService entitySearchService,
      IncidentService incidentService) {
    super(entityService, VERSION, UPGRADE_ID);
    _entitySearchService = entitySearchService;
    _incidentService = incidentService;
  }

  @Nonnull
  @Override
  public BootstrapStep.ExecutionMode getExecutionMode() {
    return BootstrapStep.ExecutionMode.ASYNC;
  }

  @Override
  public void upgrade(@Nonnull OperationContext opContext) throws Exception {

    int batch = 1;
    String nextScrollId = null;
    do {

      ScrollResult scrollResult =
          _entitySearchService.scroll(
              opContext,
              Collections.singletonList(Constants.INCIDENT_ENTITY_NAME),
              null,
              null,
              BATCH_SIZE,
              nextScrollId,
              "5m",
              null);
      nextScrollId = scrollResult.getScrollId();

      List<Urn> incidentsInBatch =
          scrollResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toList());

      try {
        batchMigrateIncidentsSummary(opContext, incidentsInBatch);
      } catch (Exception e) {
        log.error("Error while processing batch {} of incidents", batch, e);
      }
      batch++;

    } while (nextScrollId != null);
  }

  private void batchMigrateIncidentsSummary(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> incidentUrns) {
    for (Urn incidentUrn : incidentUrns) {
      migrateIncidentsSummary(opContext, incidentUrn);
    }
  }

  private void migrateIncidentsSummary(
      @Nonnull OperationContext opContext, @Nonnull final Urn incidentUrn) {
    // 1. Fetch incident info to get the related entity urn
    IncidentInfo incidentInfo = _incidentService.getIncidentInfo(opContext, incidentUrn);

    if (incidentInfo == null) {
      log.warn(
          String.format(
              "Failed to find incidentInfo aspect for incident with urn %s. Skipping updating incident summary for related incidents!",
              incidentUrn));
      return;
    }

    List<Urn> entityUrns = incidentInfo.getEntities();

    // 2. For each urn, fetch and update the Incidents Summary aspect.
    for (Urn urn : entityUrns) {
      addIncidentToSummary(opContext, urn, incidentUrn, incidentInfo);
    }
  }

  /**
   * Adds an incident to the IncidentsSummary aspect for a related entity. This is used to search
   * for entity by active and resolved incidents.
   */
  private void addIncidentToSummary(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn incidentUrn,
      @Nonnull final IncidentInfo info) {
    // 1. Fetch the latest incident summary for the entity
    IncidentsSummary summary = getIncidentsSummary(opContext, entityUrn);

    IncidentStatus status = info.getStatus();
    IncidentSummaryDetails details = buildIncidentSummaryDetails(incidentUrn, info);

    // 2. Add the incident to active or resolved incidents
    if (IncidentState.ACTIVE.equals(status.getState())) {
      // First, ensure this isn't in any summaries anymore.
      IncidentsSummaryUtils.removeIncidentFromResolvedSummary(incidentUrn, summary);
      IncidentsSummaryUtils.removeIncidentFromActiveSummary(incidentUrn, summary);

      // Then, add to active.
      IncidentsSummaryUtils.addIncidentToActiveSummary(details, summary, 100);
    } else if (IncidentState.RESOLVED.equals(status.getState())) {
      // First, ensure this isn't in any summaries anymore.
      IncidentsSummaryUtils.removeIncidentFromActiveSummary(incidentUrn, summary);
      IncidentsSummaryUtils.removeIncidentFromResolvedSummary(incidentUrn, summary);

      // Then, add to resolved.
      IncidentsSummaryUtils.addIncidentToResolvedSummary(details, summary, 100);
    }

    // 3. Emit the change back!
    updateIncidentSummary(opContext, entityUrn, summary);
  }

  @Nonnull
  private IncidentsSummary getIncidentsSummary(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    IncidentsSummary maybeIncidentsSummary =
        _incidentService.getIncidentsSummary(opContext, entityUrn);
    return maybeIncidentsSummary == null
        ? new IncidentsSummary()
            .setResolvedIncidentDetails(new IncidentSummaryDetailsArray())
            .setActiveIncidentDetails(new IncidentSummaryDetailsArray())
        : maybeIncidentsSummary;
  }

  @Nonnull
  private IncidentSummaryDetails buildIncidentSummaryDetails(
      @Nonnull final Urn urn, @Nonnull final IncidentInfo info) {
    IncidentSummaryDetails incidentSummaryDetails = new IncidentSummaryDetails();
    incidentSummaryDetails.setUrn(urn);
    incidentSummaryDetails.setCreatedAt(info.getCreated().getTime());
    if (IncidentType.CUSTOM.equals(info.getType())) {
      incidentSummaryDetails.setType(info.getCustomType());
    } else {
      incidentSummaryDetails.setType(info.getType().toString());
    }
    if (info.hasPriority()) {
      incidentSummaryDetails.setPriority(info.getPriority());
    }
    if (IncidentState.RESOLVED.equals(info.getStatus().getState())) {
      incidentSummaryDetails.setResolvedAt(info.getStatus().getLastUpdated().getTime());
    }
    return incidentSummaryDetails;
  }

  /** Updates the incidents summary for a given entity */
  private void updateIncidentSummary(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final IncidentsSummary newSummary) {
    try {
      _incidentService.updateIncidentsSummary(opContext, entityUrn, newSummary);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to updated incidents summary for entity with urn %s! Skipping updating the summary",
              entityUrn),
          e);
    }
  }
}
