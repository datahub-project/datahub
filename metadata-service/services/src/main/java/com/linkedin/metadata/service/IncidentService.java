package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.IncidentKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IncidentService extends BaseService {

  public IncidentService(@Nonnull final SystemEntityClient entityClient) {
    super(entityClient);
  }

  /**
   * Returns an instance of {@link IncidentInfo} for the specified Incident urn, or null if one
   * cannot be found.
   *
   * @param incidentUrn the urn of the Incident
   * @return an instance of {@link IncidentInfo} for the Incident, null if it does not exist.
   */
  @Nullable
  public IncidentInfo getIncidentInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn incidentUrn) {
    Objects.requireNonNull(incidentUrn, "incidentUrn must not be null");
    final EntityResponse response = getIncidentEntityResponse(opContext, incidentUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.INCIDENT_INFO_ASPECT_NAME)) {
      return new IncidentInfo(
          response.getAspects().get(Constants.INCIDENT_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link IncidentsSummary} for the specified Entity urn, or null if one
   * cannot be found.
   *
   * @param entityUrn the urn of the entity to retrieve the summary for
   * @return an instance of {@link IncidentsSummary} for the Entity, null if it does not exist.
   */
  @Nullable
  public IncidentsSummary getIncidentsSummary(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    final EntityResponse response = getIncidentsSummaryResponse(opContext, entityUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.INCIDENTS_SUMMARY_ASPECT_NAME)) {
      return new IncidentsSummary(
          response.getAspects().get(Constants.INCIDENTS_SUMMARY_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Produces a Metadata Change Proposal to update the IncidentsSummary aspect for a given entity.
   */
  public void updateIncidentsSummary(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final IncidentsSummary newSummary)
      throws Exception {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(newSummary, "newSummary must not be null");
    this.entityClient.ingestProposal(
        opContext,
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, Constants.INCIDENTS_SUMMARY_ASPECT_NAME, newSummary),
        false);
  }

  /** Deletes an incident with a given URN */
  public void deleteIncident(@Nonnull OperationContext opContext, @Nonnull final Urn incidentUrn)
      throws Exception {
    Objects.requireNonNull(incidentUrn, "incidentUrn must not be null");
    this.entityClient.deleteEntity(opContext, incidentUrn);
    this.entityClient.deleteEntityReferences(opContext, incidentUrn);
  }

  /** Updates an existing incident's status. */
  public Urn raiseIncident(
      @Nonnull OperationContext opContext,
      @Nonnull final IncidentType type,
      @Nullable final String customType,
      @Nullable final Integer priority,
      @Nullable final String title,
      @Nullable final String description,
      @Nonnull final List<Urn> entityUrns,
      @Nullable final IncidentSource source,
      @Nonnull final Urn actor,
      @Nullable final String message)
      throws Exception {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(entityUrns, "entityUrns must not be null");
    Objects.requireNonNull(actor, "actor must not be null");

    final IncidentKey key = new IncidentKey();
    final String id = UUID.randomUUID().toString();
    key.setId(id);
    final Urn urn = EntityKeyUtils.convertEntityKeyToUrn(key, Constants.INCIDENT_ENTITY_NAME);

    final IncidentInfo newInfo = new IncidentInfo();
    newInfo.setType(type);
    newInfo.setCustomType(customType, SetMode.IGNORE_NULL);
    newInfo.setPriority(priority, SetMode.IGNORE_NULL);
    newInfo.setTitle(title, SetMode.IGNORE_NULL);
    newInfo.setDescription(description, SetMode.IGNORE_NULL);
    newInfo.setEntities(new UrnArray(entityUrns));
    newInfo.setSource(source, SetMode.IGNORE_NULL);
    newInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setMessage(message, SetMode.IGNORE_NULL)
            .setLastUpdated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis())));
    newInfo.setCreated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()));
    this.entityClient.ingestProposal(
        opContext,
        AspectUtils.buildMetadataChangeProposal(urn, Constants.INCIDENT_INFO_ASPECT_NAME, newInfo),
        false);
    return urn;
  }

  /** Updates an existing incident's status. */
  public void updateIncidentStatus(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final IncidentState state,
      @Nonnull final Urn actor,
      @Nullable final String message)
      throws Exception {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(state, "state must not be null");
    Objects.requireNonNull(actor, "actor must not be null");
    final IncidentInfo existingInfo = getIncidentInfo(opContext, urn);
    if (existingInfo != null) {
      final IncidentStatus newStatus =
          new IncidentStatus()
              .setState(state)
              .setLastUpdated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()))
              .setMessage(message, SetMode.IGNORE_NULL);
      existingInfo.setStatus(newStatus);
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(
              urn, Constants.INCIDENT_INFO_ASPECT_NAME, existingInfo),
          false);
    } else {
      throw new IllegalArgumentException(
          String.format("Failed to find incident with urn %s. Incident may not exist!", urn));
    }
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn, or null if one cannot
   * be found.
   *
   * @param incidentUrn the urn of the View
   * @param authentication the authentication to use
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  private EntityResponse getIncidentEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn incidentUrn) {
    Objects.requireNonNull(incidentUrn, "incidentUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.INCIDENT_ENTITY_NAME,
          incidentUrn,
          ImmutableSet.of(Constants.INCIDENT_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Incident with urn %s", incidentUrn), e);
    }
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified Entity urn containing the
   * incidents summary aspect or null if one cannot be found.
   *
   * @param entityUrn the urn of the Entity for which to fetch incident summary
   * @param authentication the authentication to use
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  private EntityResponse getIncidentsSummaryResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(Constants.INCIDENTS_SUMMARY_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Incident Summary for entity with urn %s", entityUrn),
          e);
    }
  }
}
