package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalyState;
import com.linkedin.anomaly.AnomalyStatus;
import com.linkedin.anomaly.AnomalyStatusProperties;
import com.linkedin.anomaly.AnomalyType;
import com.linkedin.common.AnomaliesSummary;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.AnomalyKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;



@Slf4j
public class AnomalyService extends BaseService {

  public AnomalyService(@Nonnull final EntityClient entityClient, @Nonnull final Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Returns an instance of {@link AnomalyInfo} for the specified Anomaly urn,
   * or null if one cannot be found.
   *
   * @param anomalyUrn the urn of the Anomaly
   *
   * @return an instance of {@link AnomalyInfo} for the Anomaly, null if it does not exist.
   */
  @Nullable
  public AnomalyInfo getAnomalyInfo(@Nonnull final Urn anomalyUrn) {
    Objects.requireNonNull(anomalyUrn, "anomalyUrn must not be null");
    final EntityResponse response = getAnomalyEntityResponse(anomalyUrn);
    if (response != null && response.getAspects().containsKey(Constants.ANOMALY_INFO_ASPECT_NAME)) {
      return new AnomalyInfo(response.getAspects().get(Constants.ANOMALY_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link AnomaliesSummary} for the specified Entity urn,
   * or null if one cannot be found.
   *
   * @param entityUrn the urn of the entity to retrieve the summary for
   *
   * @return an instance of {@link AnomaliesSummary} for the Entity, null if it does not exist.
   */
  @Nullable
  public AnomaliesSummary getAnomaliesSummary(@Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    final EntityResponse response = getAnomaliesSummaryResponse(entityUrn);
    if (response != null && response.getAspects().containsKey(Constants.ANOMALIES_SUMMARY_ASPECT_NAME)) {
      return new AnomaliesSummary(response.getAspects().get(Constants.ANOMALIES_SUMMARY_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Produces a Metadata Change Proposal to update the Anomalies Summary aspect for a given entity.
   */
  public void updateAnomaliesSummary(@Nonnull final Urn entityUrn, @Nonnull final AnomaliesSummary newSummary) throws Exception {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(newSummary, "newSummary must not be null");
    this.entityClient.ingestProposal(
        AspectUtils.buildMetadataChangeProposal(entityUrn, Constants.ANOMALIES_SUMMARY_ASPECT_NAME, newSummary),
        this.systemAuthentication,
        false);
  }

  /**
   * Deletes an anomaly with a given URN
   */
  public void deleteAnomaly(@Nonnull final Urn anomalyUrn) throws Exception {
    Objects.requireNonNull(anomalyUrn, "anomalyUrn must not be null");
    this.entityClient.deleteEntity(anomalyUrn, this.systemAuthentication);
    this.entityClient.deleteEntityReferences(anomalyUrn, this.systemAuthentication);
  }

  /**
   * Raises a new anomaly for an asset
   */
  public Urn raiseAnomaly(
      @Nonnull final AnomalyType type,
      @Nullable final Integer severity,
      @Nullable final String description,
      @Nonnull final Urn entityUrn,
      @Nonnull final AnomalySource source,
      @Nonnull final Urn actor) throws Exception {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(actor, "actor must not be null");

    final AnomalyKey key = new AnomalyKey();
    final String id = UUID.randomUUID().toString();
    key.setId(id);
    final Urn urn = EntityKeyUtils.convertEntityKeyToUrn(key, Constants.ANOMALY_ENTITY_NAME);

    final AnomalyInfo newInfo = new AnomalyInfo();
    newInfo.setType(type);
    newInfo.setDescription(description, SetMode.IGNORE_NULL);
    newInfo.setEntity(entityUrn);
    newInfo.setSource(source, SetMode.IGNORE_NULL);
    newInfo.setSeverity(severity, SetMode.IGNORE_NULL);
    newInfo.setStatus(new AnomalyStatus()
        .setState(AnomalyState.ACTIVE)
        .setLastUpdated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()))
    );
    newInfo.setCreated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()));
    this.entityClient.ingestProposal(
        AspectUtils.buildMetadataChangeProposal(urn, Constants.ANOMALY_INFO_ASPECT_NAME, newInfo),
        this.systemAuthentication,
        false);
    return urn;
  }

  /**
   * Updates an existing anomaly's status.
   */
  public void updateAnomalyStatus(
      @Nonnull final Urn urn,
      @Nonnull final AnomalyState state,
      @Nullable final AnomalyStatusProperties properties,
      @Nonnull final Urn actor) throws Exception {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(state, "state must not be null");
    Objects.requireNonNull(actor, "actor must not be null");
    final AnomalyInfo existingInfo = getAnomalyInfo(urn);
    if (existingInfo != null) {
      final AnomalyStatus newStatus = new AnomalyStatus()
          .setState(state)
          .setProperties(properties, SetMode.IGNORE_NULL)
          .setLastUpdated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()));
      existingInfo.setStatus(newStatus);
      this.entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(urn, Constants.ANOMALY_INFO_ASPECT_NAME, existingInfo),
          this.systemAuthentication,
          false);
    } else {
      throw new IllegalArgumentException(String.format("Failed to find anomaly with urn %s. Anomaly may not exist!", urn));
    }
  }


  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn,
   * or null if one cannot be found.
   *
   * @param anomalyUrn the urn of the View
   *
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  private EntityResponse getAnomalyEntityResponse(@Nonnull final Urn anomalyUrn) {
    Objects.requireNonNull(anomalyUrn, "anomalyUrn must not be null");
    try {
      return this.entityClient.getV2(
          Constants.ANOMALY_ENTITY_NAME,
          anomalyUrn,
          ImmutableSet.of(Constants.ANOMALY_INFO_ASPECT_NAME),
          this.systemAuthentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Anomaly with urn %s", anomalyUrn), e);
    }
  }


  /**
   * Returns an instance of {@link EntityResponse} for the specified Entity urn containing the Anomalies summary aspect
   * or null if one cannot be found.
   *
   * @param entityUrn the urn of the Entity for which to fetch anomaly summary
   *
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  private EntityResponse getAnomaliesSummaryResponse(@Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    try {
      return this.entityClient.getV2(
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(Constants.ANOMALIES_SUMMARY_ASPECT_NAME),
          this.systemAuthentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Anomalies Summary for entity with urn %s", entityUrn), e);
    }
  }
}
