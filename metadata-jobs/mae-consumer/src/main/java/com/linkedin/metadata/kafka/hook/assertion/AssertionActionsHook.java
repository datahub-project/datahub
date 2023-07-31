package com.linkedin.metadata.kafka.hook.assertion;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceProperties;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.AnomalyState;
import com.linkedin.anomaly.AnomalyStatusProperties;
import com.linkedin.anomaly.AnomalyType;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.incident.IncidentServiceFactory;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.service.AnomalyService;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.IncidentService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.Constants.*;


/**
 * This hook is responsible for applying Actions that occur on success or failure of an assertion, for example auto-raising and closing incidents
 * that are generated due to an assertion run event.
 *
 * There are a few cases covered by the hook:
 *
 * Case 1: Assertion Success Event.
 *
 *  a) Incidents: When an Assertion passes, this hook will inspect whether the assertion
 *     has been configured to resolve incidents (which were raised by the generator). If yes, then the generator will
 *     auto-close the active incidents related to the assertion. If no, then the generator will skip the assertion.
 *
 *  b) Anomalies: In contract, when an Inferred (system) Assertion passes, any active anomalies that were generated based on a
 *     previous Assertion failure will be resolved.
 *     Note that resolution is different from REVIEW. Anomalies will still be subject to review, even if they are marked in
 *     the resolved state.
 *
 * Case 2: Assertion Failure Event.
 *
 *  a) Incidents: When an Assertion fails, this hook will inspect whether the assertion has
 *     been configured to raise incidents. If yes, then the generator will auto-raise an incident related to this
 *     assertion. If no, then the generator will skip the assertion.
 *
 *  a) Anomalies: When an Inferred (system) Assertion fails, this hook will generate an Anomaly which reflects
 *     what has been detected.
 *     In the future, we intend to add ability to disable anomaly generation for particular assets, or globally.
 *     If there is an active anomaly which has already been generated based on the results of the assertion,
 *     it will simply be updated to reflect the current state.
 *     We will NOT generate duplicate anomalies based on the same inferred assertion.
 *     In the future, we will likely need to prevent new anomalies from being generated if existing anomalies were
 *     manually reviewed / resolved recently. (To prevent a thrashing between Active and Resolved anomalies until the models are
 *     able to update).
 *
 * Case 3: An Assertion is Hard Deleted.
 *
 *  a) Incidents: When an Assertion is deleted, this hook will search for any
 *     active or resolved incidents that were generated by the Assertion. It will then cascade the delete to the incident,
 *     deleting it in it's entirety.
 *
 *  b) Anomalies: When an Inferred Assertion is deleted, this hook will search for any active anomalies that were
 *     generated by the Assertion. It will then cascade the delete to the anomalies, deleting it in it's entirety.
 *
 * Case 4: An Assertion is Soft Deleted.
 *
 *  a) Incidents: When an Assertion is deleted, this hook will search for any
 *      active or resolved incidents that were generated by the Assertion. It will then cascade the delete to the incident,
 *      deleting it in it's entirety.
 *
 *  b) Anomalies: When an Inferred Assertion is deleted, this hook will search for any active anomalies that were
 *     generated by the Assertion. It will then cascade the delete to the anomalies, deleting it in it's entirety.
 *
 * The hook handles restated events by replaying the logic. The hook will ensure that multiple incidents are not
 * generated by the SAME assertion at once (prevents duplicate incidents).
 */
@Slf4j
@Component
@Singleton
@Import({
    EntityRegistryFactory.class,
    RestliEntityClientFactory.class,
    IncidentServiceFactory.class,
    AssertionServiceFactory.class,
    SystemAuthenticationFactory.class})
public class AssertionActionsHook implements MetadataChangeLogHook {

  private static final String INCIDENT_SOURCE_URN_SEARCH_INDEX_FIELD_NAME = "sourceUrn.keyword";
  private static final String INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME = "state";
  private static final String INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entities.keyword";
  private static final String ANOMALY_SOURCE_URN_SEARCH_INDEX_FIELD_NAME = "sourceUrn.keyword";
  private static final String ANOMALY_STATE_SEARCH_INDEX_FIELD_NAME = "state";
  private static final String ANOMALY_ENTITY_SEARCH_INDEX_FIELD_NAME = "entity.keyword";
  private static final int SEARCH_BATCH_SIZE = 1000;

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES = ImmutableSet.of(
      ChangeType.UPSERT,
      ChangeType.CREATE,
      ChangeType.RESTATE);

  private final EntityRegistry _entityRegistry;
  private final EntityClient _entityClient;
  private final AssertionService _assertionService;
  private final IncidentService _incidentService;
  private final AnomalyService _anomalyService;
  private final Authentication _authentication;
  private final boolean _isEnabled;

  @Autowired
  public AssertionActionsHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull @Qualifier("restliEntityClient") final EntityClient entityClient,
      @Nonnull final Authentication authentication,
      @Nonnull @Value("${assertionActions.hook.enabled:true}") Boolean isEnabled
  ) {
    _entityRegistry = Objects.requireNonNull(entityRegistry, "entityRegistry is required");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient is entityClient");
    _authentication = Objects.requireNonNull(authentication, "authentication is required");
    _assertionService = new AssertionService(entityClient, authentication);
    _incidentService = new IncidentService(entityClient, authentication);
    _anomalyService = new AnomalyService(entityClient, authentication);
    _isEnabled = isEnabled;
  }

  @Override
  public void init() {
  }

  @Override
  public boolean isEnabled() {
    return _isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (_isEnabled) {
      log.debug("Urn {} received by Assertion Actions Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, _entityRegistry);
      if (isAssertionSoftDeleted(event) || isAssertionHardDeleted(event)) {
        // Handle the deletion case.
        handleAssertionDeleted(urn);
      } else if (isAssertionRunResultEvent(event)) {
        // Handle the run event case.
        handleAssertionRunResult(urn, event);
      }
      // Otherwise, we skip the event.
    }
  }

  /**
   * Handles an assertion deletion by finding and removing any ACTIVE incidents that were generated based on it's results.
   * Note that existing incidents (e.g. those which were resolved), will continue to live in history.
   */
  private void handleAssertionDeleted(@Nonnull final Urn assertionUrn) {
    // Case 1: Remove any active incidents that were raised as a direct result of the assertion being deleted.
    tryDeleteActiveIncidents(assertionUrn);
    // Case 2: Remove any active anomalies that were generated as a direct result of the assertion being deleted.
    tryDeleteActiveAnomalies(assertionUrn);
  }

  /**
   * Handle an assertion run result by optionally raising an incident for it.
   */
  private void handleAssertionRunResult(@Nonnull final Urn assertionUrn, @Nonnull final MetadataChangeLog event) {
    // 1. Parse the event into a run event
    final AssertionRunEvent runEvent = GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        AssertionRunEvent.class
    );

    // 2. Apply actions based on run result.
    if (isAssertionRunCompleted(runEvent)) {
      // Assertion run is complete - apply post-result actions.=
      applyAssertionActions(assertionUrn, runEvent);
    }
    // Assertion not completed. Nothing to do.
  }

  private void applyAssertionActions(@Nonnull final Urn assertionUrn, @Nonnull final AssertionRunEvent runEvent) {
    final AssertionInfo info = _assertionService.getAssertionInfo(assertionUrn);
    if (info != null) {
      if (AssertionResultType.SUCCESS.equals(runEvent.getResult().getType())) {
        applyAssertionSuccessActions(assertionUrn, runEvent, info);
      } else if (AssertionResultType.FAILURE.equals(runEvent.getResult().getType())) {
        applyAssertionFailureActions(assertionUrn, runEvent, info);
      }
    } else {
      log.warn(String.format("Could not find assertion with urn %s. Skipping applying actions!", assertionUrn));
    }
  }

  private void applyAssertionSuccessActions(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info) {
    // 1. Fetch the assertion info to retrieve any actions
    final AssertionActions actions = _assertionService.getAssertionActions(assertionUrn);

    // 2. Ensure that assertion exists & has actions
    if (actions != null && actions.hasOnSuccess()) {
      actions.getOnSuccess().forEach(action -> applyAssertionAction(assertionUrn, runEvent, info, action));
    }

    // 3. Always Apply System Anomaly Actions
    applyResolveAnomaliesAction(assertionUrn, runEvent, info);
  }

  private void applyAssertionFailureActions(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info) {
    // 1. Fetch the assertion info to retrieve any actions
    final AssertionActions actions = _assertionService.getAssertionActions(assertionUrn);

    // 2. Ensure that assertion exists & has actions
    if (actions != null && actions.hasOnFailure()) {
      actions.getOnFailure().forEach(action -> applyAssertionAction(assertionUrn, runEvent, info, action));
    }

    // 3. Always Apply System Anomaly Actions
    applyRaiseAnomalyAction(assertionUrn, runEvent, info);
  }

  private void applyAssertionAction(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionAction action) {
    switch (action.getType()) {
      case RAISE_INCIDENT:
        applyRaiseIncidentAction(assertionUrn, runEvent, info);
        break;
      case RESOLVE_INCIDENT:
        applyResolveIncidentAction(assertionUrn, runEvent, info);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Failed to apply assertion action. Unrecognized action type %s provided!", action.getType()));
    }
  }

  private void applyRaiseIncidentAction(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info) {
    // Raises an incident on the entity targeted by the assertion.
    final Urn entityUrn = getAsserteeUrn(info);

    // Check whether an incident is already active for this entity + assertion pair.
    if (!hasActiveAssertionIncident(entityUrn, assertionUrn)) {
      raiseIncident(entityUrn, assertionUrn, runEvent, info);
    }
  }

  private void applyResolveIncidentAction(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info) {
    final Urn entityUrn = getAsserteeUrn(info);
    // 1. Get the urns of any active incidents resulting from this assertion.
    try {
      final SearchResult searchResult =
          _entityClient.search(INCIDENT_ENTITY_NAME, "*", buildActiveEntityIncidentsFilter(entityUrn, assertionUrn),
              null, 0, SEARCH_BATCH_SIZE, // This SHOULD NOT exceed 1 in reality.
              _authentication, new SearchFlags().setFulltext(false).setSkipCache(true));

      // 2. If there are active incidents, resolve them.
      if (searchResult.hasEntities() && searchResult.getEntities().size() > 0) {
        searchResult.getEntities().forEach(entity -> resolveIncident(entity.getEntity(), assertionUrn, runEvent));
      }
    } catch (Exception e) {
      // Could not determine whether there are active assertion related incidents. Just throw.
      throw new RuntimeException(String.format(
          "Caught exception while attempting to resolve active incident related to entity urn %s and assertion urn %s. This means"
              + " that active incidents may need to be resolved manually!", entityUrn, assertionUrn), e);
    }
  }

  private void applyRaiseAnomalyAction(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull AssertionInfo info) {
    // Raises an anomaly on the entity targeted by the assertion.
    if (isInferredAssertion(info)) {
      final Urn entityUrn = getAsserteeUrn(info);

      // Check whether an anomaly is already active for this entity + assertion pair.
      if (!hasActiveAssertionAnomaly(entityUrn, assertionUrn)) {
        raiseAnomaly(entityUrn, assertionUrn, runEvent, info);
      }
    }
  }

  private void applyResolveAnomaliesAction(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info) {
    // Resolves any active anomalies on the entity targeted by the assertion.
    if (isInferredAssertion(info)) {
      final Urn entityUrn = getAsserteeUrn(info);
      // 1. Get the urns of any active anomalies resulting from this assertion.
      try {
        final SearchResult searchResult = _entityClient.search(
            ANOMALY_ENTITY_NAME,
            "*",
            buildActiveEntityAnomaliesFilter(entityUrn, assertionUrn),
            null,
            0,
            SEARCH_BATCH_SIZE, // This SHOULD NOT exceed 1 in reality.
            _authentication,
            new SearchFlags().setSkipCache(true));

        // 2. If there are active anomalies, resolve them.
        if (searchResult.hasEntities() && searchResult.getEntities().size() > 0) {
          searchResult.getEntities().forEach(entity -> resolveAnomaly(entity.getEntity(), assertionUrn, runEvent));
        }

      } catch (Exception e) {
        // Could not determine whether there are active assertion related anomalies. Just throw.
        throw new RuntimeException(String.format(
            "Caught exception while attempting to resolve active assertion related to entity urn %s and assertion urn %s. This means"
                + " that active anomalies may need to be resolved manually!", entityUrn, assertionUrn), e);
      }
    }
  }

  /**
   * Raises an incident for an entity, related to an assertion run result.
   *
   * @param entityUrn the urn of the entity to raise the incident on
   * @param assertionUrn the urn of the assertion with the result
   */
  private void raiseIncident(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info) {
    try {
      _incidentService.raiseIncident(
          getIncidentTypeFromAssertionInfo(info),
          null,
          0,
          "A critical Assertion is failing for this asset.",
          "A critical Assertion has failed for this data asset. This may indicate that the asset is unhealthy or unfit for consumption!",
          ImmutableList.of(entityUrn),
          new IncidentSource()
            .setSourceUrn(assertionUrn)
            .setType(IncidentSourceType.ASSERTION_FAILURE),
          UrnUtils.getUrn(SYSTEM_ACTOR),
          "Auto-Raised: This incident was automatically generated by a failing assertion."
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Failed to raise incident on entity %s associated with assertion %s! This means that an active issue may go unnoticed.",
          entityUrn,
          assertionUrn), e);
    }
  }

  /**
   * Resolves an active incident related to an assertion run result.
   *
   * @param incidentUrn the urn of the incident to resolve
   * @param assertionUrn the urn of the assertion with the result
   */
  private void resolveIncident(@Nonnull final Urn incidentUrn, @Nonnull final Urn assertionUrn, @Nonnull final AssertionRunEvent runEvent) {
    try {
      _incidentService.updateIncidentStatus(
          incidentUrn,
          IncidentState.RESOLVED,
          UrnUtils.getUrn(SYSTEM_ACTOR),
          "Auto-Resolved: The failing assertion which generated this incident is now passing."
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Failed to resolve incident %s associated with assertion %s! This means a stale incident may be unresolved.",
          incidentUrn,
          assertionUrn), e);
    }
  }

  /**
   * Raises an anomaly for an entity, related to an assertion run result.
   *
   * @param entityUrn the urn of the entity to raise the anomaly on
   * @param assertionUrn the urn of the assertion with the result
   */
  private void raiseAnomaly(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo info) {
    try {
      _anomalyService.raiseAnomaly(
          getAnomalyTypeFromAssertionInfo(info),
          0,
          AnomalyUtils.generateAnomalyDescription(info, runEvent),
          entityUrn,
          new AnomalySource()
              .setSourceUrn(assertionUrn)
              .setProperties(new AnomalySourceProperties().setAssertionRunEventTime(runEvent.getTimestampMillis()))
              .setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE),
          UrnUtils.getUrn(SYSTEM_ACTOR)
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Failed to raise anomaly on entity %s associated with assertion %s! This means that an active issue may go unnoticed.",
          entityUrn,
          assertionUrn), e);
    }
  }

  /**
   * Resolves an active anomaly related to an assertion run result.
   *
   * @param anomalyUrn the urn of the anomaly to resolve
   * @param assertionUrn the urn of the assertion with the result
   */
  private void resolveAnomaly(@Nonnull final Urn anomalyUrn, @Nonnull final Urn assertionUrn, @Nonnull final AssertionRunEvent runEvent) {
    try {
      _anomalyService.updateAnomalyStatus(
          anomalyUrn,
          AnomalyState.RESOLVED,
          new AnomalyStatusProperties().setAssertionRunEventTime(runEvent.getTimestampMillis()),
          UrnUtils.getUrn(SYSTEM_ACTOR)
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Failed to resolve anomaly %s associated with assertion %s! This means a stale anomaly may be unresolved.",
          anomalyUrn,
          assertionUrn), e);
    }
  }

  private boolean hasActiveAssertionIncident(@Nonnull final Urn entityUrn, @Nonnull final Urn assertionUrn) {
    try {
      final SearchResult searchResult = _entityClient.search(
          INCIDENT_ENTITY_NAME,
          "*",
          buildActiveEntityIncidentsFilter(entityUrn, assertionUrn),
          null,
          0, SEARCH_BATCH_SIZE, // This SHOULD NOT exceed 1 in reality.
          _authentication,
          new SearchFlags().setFulltext(false).setSkipCache(true));

      return searchResult.hasEntities() && searchResult.getEntities().size() > 0;
    } catch (Exception e) {
      // Could not determine whether there are active assertion related incidents. Just throw.
      throw new RuntimeException(String.format(
          "Caught exception while attempting to retrieve active assertion related to entity urn and assertion urn %s", entityUrn, assertionUrn), e);
    }
  }

  private boolean hasActiveAssertionAnomaly(@Nonnull final Urn entityUrn, @Nonnull final Urn assertionUrn) {
    try {
      final SearchResult searchResult = _entityClient.search(
          ANOMALY_ENTITY_NAME,
          "*",
          buildActiveEntityAnomaliesFilter(entityUrn, assertionUrn),
          null,
          0,
          SEARCH_BATCH_SIZE, // This SHOULD NOT exceed 1 in reality.
          _authentication,
          new SearchFlags().setSkipCache(true));

      return searchResult.hasEntities() && searchResult.getEntities().size() > 0;
    } catch (Exception e) {
      // Could not determine whether there are active assertion related anomalies. Just throw.
      throw new RuntimeException(String.format(
          "Caught exception while attempting to retrieve active assertion related to entity urn %s and assertion urn %s", entityUrn, assertionUrn), e);
    }
  }

  private void tryDeleteActiveIncidents(@Nonnull final Urn assertionUrn) {
    try {
      deleteActiveIncidents(assertionUrn);
    } catch (Exception e) {
      log.error(
        String.format("Caught exception while attempting to delete active incidents associated with removed assertion urn %s! "
            + "This means that a stale incident may remain active.", assertionUrn),
        e);
    }
  }

  private void tryDeleteActiveAnomalies(@Nonnull final Urn assertionUrn) {
    try {
      deleteActiveAnomalies(assertionUrn);
    } catch (Exception e) {
      log.error(
          String.format("Caught exception while attempting to delete active anomalies associated with removed assertion urn %s! "
              + "This means that a stale anomaly may remain active.", assertionUrn),
          e);
    }
  }

  private void deleteActiveIncidents(@Nonnull final Urn assertionUrn) throws Exception {
    // Fetch any incidents that were raised as a result of the assertion.
    // Note that there should never be more than 1 active incident associated with this assertion urn (as of today).
    // If we ever support multi-entity assertions, this will need to change.
    final SearchResult searchResult = _entityClient.search(
        INCIDENT_ENTITY_NAME,
        "*",
        buildActiveIncidentsFilter(assertionUrn),
        null,
        0, SEARCH_BATCH_SIZE, // This SHOULD NOT exceed 1 in reality.
        _authentication,
        new SearchFlags().setFulltext(false).setSkipCache(true));

    if (searchResult.hasEntities() && searchResult.getEntities().size() > 0) {
      log.info(String.format(
          "Found %s active incident(s) associated with assertion being removed urn %s. Removing incidents %s",
          searchResult.getEntities().size(),
          assertionUrn,
          searchResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList())
      ));
      searchResult.getEntities().forEach(res -> {
        // We have found an active incident. Let's remove it (along with any references to it).
        final Urn incidentUrn = res.getEntity();
        try {
          // Now simply delete the incident.
          _incidentService.deleteIncident(incidentUrn);
        } catch (Exception e) {
          log.error(String.format(
              "Caught exception while attempting to delete incident with urn %s. This may mean that the incident", incidentUrn)
              + "or references to it could not be removed, resulting in stale information!");
          // Do not throw so that processing can continue.
        }
      });
    }
    // Otherwise, we haven't found any active incidents. Simply continue.
  }

  private void deleteActiveAnomalies(@Nonnull final Urn assertionUrn) throws Exception {
    // Fetch any anomalies that were raised as a result of the assertion.
    // Note that there should never be more than 1 active anomaly associated with this assertion urn (as of today).
    final SearchResult searchResult = _entityClient.search(
        ANOMALY_ENTITY_NAME,
        "*",
        buildActiveAnomaliesFilter(assertionUrn),
        null,
        0, SEARCH_BATCH_SIZE, // This SHOULD NOT exceed 1 in reality.
        _authentication,
        new SearchFlags().setSkipCache(true).setFulltext(false));

    if (searchResult.hasEntities() && searchResult.getEntities().size() > 0) {
      log.info(String.format(
          "Found %s active incident(s) associated with assertion being removed urn %s. Removing incidents %s",
          searchResult.getEntities().size(),
          assertionUrn,
          searchResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList())
      ));
      searchResult.getEntities().forEach(res -> {
        // We have found an active anomaly. Let's remove it (along with any references to it).
        final Urn anomalyUrn = res.getEntity();
        try {
          // Now simply delete the anomaly.
          _anomalyService.deleteAnomaly(anomalyUrn);
        } catch (Exception e) {
          log.error(String.format(
              "Caught exception while attempting to delete anomaly with urn %s. This may mean that the anomaly", anomalyUrn)
              + " or references to it could not be removed, resulting in stale information!");
          // Do not throw so that processing can continue.
        }
      });
    }
    // Otherwise, we haven't found any active anomalies. Simply continue.
  }

  private boolean isAssertionRunCompleted(@Nonnull final AssertionRunEvent runEvent) {
    return AssertionRunStatus.COMPLETE.equals(runEvent.getStatus()) && runEvent.hasResult();
  }

  /**
   * Returns true if an assertion is being soft-deleted.
   */
  private boolean isAssertionSoftDeleted(@Nonnull final MetadataChangeLog event) {
    return SUPPORTED_UPDATE_TYPES.contains(event.getChangeType()) && isSoftDeletionEvent(event);
  }

  /**
   * Returns true if an assertion is being soft-deleted.
   */
  private boolean isAssertionHardDeleted(@Nonnull final MetadataChangeLog event) {
    return isHardDeletionEvent(event);
  }

  /**
   * Returns true if the event represents an incident deletion event.
   */
  private boolean isAssertionRunResultEvent(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && ASSERTION_RUN_EVENT_ASPECT_NAME.equals(event.getAspectName())
        && event.hasAspect();
  }

  private boolean isSoftDeletionEvent(@Nonnull final MetadataChangeLog event) {
    if (STATUS_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      final Status status = GenericRecordUtils.deserializeAspect(
          event.getAspect().getValue(),
          event.getAspect().getContentType(),
          Status.class);
      return status.hasRemoved() && status.isRemoved();
    }
    return false;
  }

  private boolean isHardDeletionEvent(@Nonnull final MetadataChangeLog event) {
    return ChangeType.DELETE.equals(event.getChangeType()) && ASSERTION_KEY_ASPECT_NAME.equals(event.getAspectName());
  }

  private boolean isInferredAssertion(@Nonnull final AssertionInfo assertionInfo) {
    return assertionInfo.hasSource() && AssertionSourceType.INFERRED.equals(assertionInfo.getSource().getType());
  }

  private Filter buildActiveIncidentsFilter(@Nonnull final Urn assertionUrn) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_SOURCE_URN_SEARCH_INDEX_FIELD_NAME, assertionUrn.toString());
    criterionMap.put(INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME, IncidentState.ACTIVE.toString());
    return QueryUtils.newFilter(criterionMap);
  }

  private Filter buildActiveEntityIncidentsFilter(@Nonnull final Urn entityUrn, @Nonnull final Urn assertionUrn) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME, entityUrn.toString());
    criterionMap.put(INCIDENT_SOURCE_URN_SEARCH_INDEX_FIELD_NAME, assertionUrn.toString());
    criterionMap.put(INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME, IncidentState.ACTIVE.toString());
    return QueryUtils.newFilter(criterionMap);
  }

  private Filter buildActiveAnomaliesFilter(@Nonnull final Urn assertionUrn) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(ANOMALY_SOURCE_URN_SEARCH_INDEX_FIELD_NAME, assertionUrn.toString());
    criterionMap.put(ANOMALY_STATE_SEARCH_INDEX_FIELD_NAME, IncidentState.ACTIVE.toString());
    return QueryUtils.newFilter(criterionMap);
  }

  private Filter buildActiveEntityAnomaliesFilter(@Nonnull final Urn entityUrn, @Nonnull final Urn assertionUrn) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(ANOMALY_ENTITY_SEARCH_INDEX_FIELD_NAME, entityUrn.toString());
    criterionMap.put(ANOMALY_SOURCE_URN_SEARCH_INDEX_FIELD_NAME, assertionUrn.toString());
    criterionMap.put(ANOMALY_STATE_SEARCH_INDEX_FIELD_NAME, AnomalyState.ACTIVE.toString());
    return QueryUtils.newFilter(criterionMap);
  }

  @Nonnull
  private Urn getAsserteeUrn(@Nonnull final AssertionInfo info) {
    switch (info.getType()) {
      case DATASET:
        return info.getDatasetAssertion().getDataset();
      case FRESHNESS:
        return info.getFreshnessAssertion().getEntity();
      default:
        throw new IllegalArgumentException("Failed to extract assertee urn from assertionInfo aspect! Unrecognized assertion type provided.");
    }
  }

  @Nonnull
  private IncidentType getIncidentTypeFromAssertionInfo(@Nonnull final AssertionInfo info) {
    switch (info.getType()) {
      case DATASET:
        return DatasetAssertionScope.DATASET_COLUMN.equals(info.getDatasetAssertion().getScope())
            ? IncidentType.DATASET_COLUMN
            : IncidentType.DATASET_ROWS;
      case FRESHNESS:
        return IncidentType.FRESHNESS;
      default:
        throw new IllegalArgumentException("Failed to map to an incident type! Unsupported Assertion type provided.");
    }
  }

  @Nonnull
  private AnomalyType getAnomalyTypeFromAssertionInfo(@Nonnull final AssertionInfo info) {
    switch (info.getType()) {
      case DATASET:
        return DatasetAssertionScope.DATASET_COLUMN.equals(info.getDatasetAssertion().getScope())
            ? AnomalyType.DATASET_COLUMN
            : AnomalyType.DATASET_ROWS;
      case FRESHNESS:
        return AnomalyType.FRESHNESS;
      default:
        throw new IllegalArgumentException("Failed to map to an anomaly type! Unsupported Assertion type provided.");
    }
  }
}
