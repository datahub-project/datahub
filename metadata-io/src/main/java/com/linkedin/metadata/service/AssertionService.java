package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.datahub.authentication.Authentication;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;



@Slf4j
public class AssertionService extends BaseService {

  public AssertionService(@Nonnull final EntityClient entityClient, @Nonnull final Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Returns an instance of {@link AssertionInfo} for the specified Assertion urn,
   * or null if one cannot be found.
   *
   * @param assertionUrn the urn of the Assertion
   *
   * @return an instance of {@link com.linkedin.assertion.AssertionInfo} for the Assertion, null if it does not exist.
   */
  @Nullable
  public AssertionInfo getAssertionInfo(@Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    final EntityResponse response = getAssertionEntityResponse(assertionUrn, this.systemAuthentication);
    if (response != null && response.getAspects().containsKey(Constants.ASSERTION_INFO_ASPECT_NAME)) {
      return new AssertionInfo(response.getAspects().get(Constants.ASSERTION_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link com.linkedin.common.AssertionsSummary} for the specified Entity urn,
   * or null if one cannot be found.
   *
   * @param entityUrn the urn of the entity to retrieve the summary for
   *
   * @return an instance of {@link com.linkedin.common.AssertionsSummary} for the Entity, null if it does not exist.
   */
  @Nullable
  public AssertionsSummary getAssertionsSummary(@Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    final EntityResponse response = getAssertionsSummaryResponse(entityUrn, this.systemAuthentication);
    if (response != null && response.getAspects().containsKey(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)) {
      return new AssertionsSummary(response.getAspects().get(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of AssertionActions for the specified Entity urn,
   * or null if one cannot be found.
   *
   * @param entityUrn the urn of the entity to retrieve the actions for
   *
   * @return an instance of AssertionActions for the Entity, null if it does not exist.
   */
  @Nullable
  public AssertionActions getAssertionActions(@Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    final EntityResponse response = getAssertionEntityResponse(entityUrn, this.systemAuthentication);
    if (response != null && response.getAspects().containsKey(Constants.ASSERTION_ACTIONS_ASPECT_NAME)) {
      return new AssertionActions(response.getAspects().get(Constants.ASSERTION_ACTIONS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Produces a Metadata Change Proposal to update the AssertionsSummary aspect for a given entity.
   */
  public void updateAssertionsSummary(@Nonnull final Urn entityUrn, @Nonnull final AssertionsSummary newSummary) throws Exception {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(newSummary, "newSummary must not be null");
    this.entityClient.ingestProposal(
        AspectUtils.buildMetadataChangeProposal(entityUrn, Constants.ASSERTIONS_SUMMARY_ASPECT_NAME, newSummary),
        this.systemAuthentication,
        false);
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn,
   * or null if one cannot be found.
   *
   * @param assertionUrn the urn of the View
   * @param authentication the authentication to use
   *
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  public EntityResponse getAssertionEntityResponse(@Nonnull final Urn assertionUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return this.entityClient.getV2(
          Constants.ASSERTION_ENTITY_NAME,
          assertionUrn,
          ImmutableSet.of(Constants.ASSERTION_INFO_ASPECT_NAME, Constants.ASSERTION_ACTIONS_ASPECT_NAME),
          authentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Assertion with urn %s", assertionUrn), e);
    }
  }


  /**
   * Returns an instance of {@link EntityResponse} for the specified Entity urn containing the assertions summary aspect
   * or null if one cannot be found.
   *
   * @param entityUrn the urn of the Entity for which to fetch assertion summary
   * @param authentication the authentication to use
   *
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  private EntityResponse getAssertionsSummaryResponse(@Nonnull final Urn entityUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return this.entityClient.getV2(
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME),
          authentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Assertion Summary for entity with urn %s", entityUrn), e);
    }
  }

  /**
   * Creates a new Dataset or DataJob Freshness Assertion for native execution by DataHub.
   * Assumes that the caller has already performed the required authorization.
   */
  @Nonnull
  public Urn createFreshnessAssertion(
      @Nonnull final Urn entityUrn,
      @Nonnull final FreshnessAssertionType type,
      @Nonnull final FreshnessAssertionSchedule schedule,
      @Nullable final AssertionActions actions,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    final Urn assertionUrn = generateAssertionUrn();

    final FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo()
      .setEntity(entityUrn)
      .setType(type)
      .setSchedule(schedule, SetMode.IGNORE_NULL);

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setFreshnessAssertion(freshnessInfo);
    assertion.setType(AssertionType.FRESHNESS);
    assertion.setSource(getNativeAssertionSource());

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to create new Freshness Assertion for entity with urn %s", entityUrn), e);
    }
  }

  /**
   * Creates a new Dataset Metrics Assertion for native execution by DataHub.
   * Assumes that the caller has already performed the required authorization.
   */
  @Nonnull
  public Urn createDatasetAssertion(
      @Nonnull final Urn datasetUrn,
      @Nonnull final DatasetAssertionScope scope,
      @Nullable final List<Urn> fields,
      @Nullable final AssertionStdAggregation aggregation,
      @Nonnull final AssertionStdOperator operator,
      @Nullable final AssertionStdParameters parameters,
      @Nullable final AssertionActions actions,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(datasetUrn, "datasetUrn must not be null");
    Objects.requireNonNull(scope, "scope must not be null");
    Objects.requireNonNull(operator, "operator must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    final Urn assertionUrn = generateAssertionUrn();

    final DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo()
        .setDataset(datasetUrn)
        .setScope(scope)
        .setFields(fields != null ? new UrnArray(fields) : null, SetMode.IGNORE_NULL)
        .setAggregation(aggregation, SetMode.IGNORE_NULL)
        .setOperator(operator)
        .setParameters(parameters, SetMode.IGNORE_NULL);

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setDatasetAssertion(datasetAssertion);
    assertion.setType(AssertionType.DATASET);
    assertion.setSource(getNativeAssertionSource());

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to create new Dataset Assertion for entity with urn %s", assertionUrn), e);
    }
  }

  /**
   * Updates an existing Dataset or DataJob Freshness Assertion for native execution by DataHub.
   * Assumes that the caller has already performed the required authorization.
   */
  @Nonnull
  public Urn updateFreshnessAssertion(
      @Nonnull final Urn assertionUrn,
      @Nonnull final FreshnessAssertionSchedule schedule,
      @Nullable final AssertionActions actions,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // 1. Check whether the Assertion exists
    AssertionInfo existingInfo = getAssertionInfo(assertionUrn);

    if (existingInfo == null) {
      throw new IllegalArgumentException(String.format("Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }

    if (!AssertionType.FRESHNESS.equals(existingInfo.getType())) {
      throw new IllegalArgumentException(String.format("Failed to update Assertion. Assertion with urn %s is not an Freshness assertion.", assertionUrn));
    }

    if (!existingInfo.hasFreshnessAssertion()) {
      throw new IllegalArgumentException(String.format("Failed to update Assertion. Freshness Assertion with urn %s is malformed!", assertionUrn));
    }

    FreshnessAssertionInfo existingFreshnessAssertion = existingInfo.getFreshnessAssertion();

    // 2. Apply changes to existing Assertion Info
    existingFreshnessAssertion.setSchedule(schedule, SetMode.IGNORE_NULL);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, existingInfo));
    if (actions != null) {
      aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    // 3. Ingest updates aspects
    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update Freshness Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Updates the actions executed for a given assertion.
   */
  @Nonnull
  public Urn updateAssertionActions(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionActions actions,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(actions, "actions must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // 1. Check whether the Assertion exists
    AssertionInfo existingInfo = getAssertionInfo(assertionUrn);

    if (existingInfo == null) {
      throw new IllegalArgumentException(String.format("Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }
    // 3. Ingest actions aspect
    try {
      this.entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions),
          authentication,
          false
      );
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update actions for Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Updates an existing Dataset Metrics Assertion for native execution by DataHub.
   * Assumes that the caller has already performed the required authorization.
   */
  @Nonnull
  public Urn updateDatasetAssertion(
      @Nonnull final Urn assertionUrn,
      @Nonnull final DatasetAssertionScope scope,
      @Nullable final List<Urn> fields,
      @Nullable final AssertionStdAggregation aggregation,
      @Nonnull final AssertionStdOperator operator,
      @Nullable final AssertionStdParameters parameters,
      @Nullable final AssertionActions actions,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(scope, "scope must not be null");
    Objects.requireNonNull(operator, "operator must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // 1. Check whether the Assertion exists
    AssertionInfo existingInfo = getAssertionInfo(assertionUrn);

    if (existingInfo == null) {
      throw new IllegalArgumentException(String.format("Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }

    if (!AssertionType.DATASET.equals(existingInfo.getType())) {
      throw new IllegalArgumentException(String.format("Failed to update Assertion. Assertion with urn %s is not an Dataset assertion.", assertionUrn));
    }

    if (!existingInfo.hasDatasetAssertion()) {
      throw new IllegalArgumentException(String.format("Failed to update Assertion. Dataset Assertion with urn %s is malformed!", assertionUrn));
    }

    DatasetAssertionInfo existingDatasetAssertion = existingInfo.getDatasetAssertion();

    // 2. Apply changes to existing Assertion Info
    existingDatasetAssertion.setScope(scope);
    existingDatasetAssertion.setFields(fields != null ? new UrnArray(fields) : null, SetMode.IGNORE_NULL);
    existingDatasetAssertion.setAggregation(aggregation, SetMode.IGNORE_NULL);
    existingDatasetAssertion.setOperator(operator);
    existingDatasetAssertion.setParameters(parameters, SetMode.IGNORE_NULL);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, existingInfo));
    if (actions != null) {
      aspects.add(AspectUtils.buildMetadataChangeProposal(assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    // 3. Ingest updates aspects
    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update Dataset Assertion with urn %s", assertionUrn), e);
    }
  }

  @Nonnull
  private AssertionSource getNativeAssertionSource() {
    final AssertionSource source = new AssertionSource();
    source.setType(AssertionSourceType.NATIVE);
    return source;
  }

  @Nonnull
  private Urn generateAssertionUrn() {
    final AssertionKey key = new AssertionKey();
    final String id = UUID.randomUUID().toString();
    key.setAssertionId(id);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.ASSERTION_ENTITY_NAME);
  }
}