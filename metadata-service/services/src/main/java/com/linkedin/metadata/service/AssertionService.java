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
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.SchemaAssertionCompatibility;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertionService extends BaseService {

  private final Clock _clock;

  public AssertionService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final Clock clock) {
    super(entityClient);
    _clock = clock;
  }

  public AssertionService(
      @Nonnull final SystemEntityClient entityClient) {
    super(entityClient);
    _clock = Clock.systemUTC();
  }

  /**
   * Returns an instance of {@link AssertionInfo} for the specified Assertion urn, or null if one
   * cannot be found.
   *
   * @param assertionUrn the urn of the Assertion
   * @return an instance of {@link AssertionInfo} for the Assertion, null if it does not exist.
   */
  @Nullable
  public AssertionInfo getAssertionInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    final EntityResponse response = getAssertionEntityResponse(opContext, assertionUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.ASSERTION_INFO_ASPECT_NAME)) {
      return new AssertionInfo(
          response.getAspects().get(Constants.ASSERTION_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of AssertionActions for the specified Entity urn, or null if one cannot be
   * found.
   *
   * @param entityUrn the urn of the entity to retrieve the actions for
   * @return an instance of AssertionActions for the Entity, null if it does not exist.
   */
  @Nullable
  public AssertionActions getAssertionActions(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    final EntityResponse response = getAssertionEntityResponse(opContext, entityUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.ASSERTION_ACTIONS_ASPECT_NAME)) {
      return new AssertionActions(
          response.getAspects().get(Constants.ASSERTION_ACTIONS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link com.linkedin.common.DataPlatformInstance} for the specified
   * Assertion urn, or null if one cannot be found.
   *
   * @param assertionUrn the urn of the Assertion
   * @return an instance of {@link AssertionInfo} for the Assertion, null if it does not exist.
   */
  @Nullable
  public DataPlatformInstance getAssertionDataPlatformInstance(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    final EntityResponse response = getAssertionEntityResponse(opContext, assertionUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME)) {
      return new DataPlatformInstance(
          response
              .getAspects()
              .get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME)
              .getValue()
              .data());
    }
    // No aspect found
    return null;
  }


  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn, or null if one cannot
   * be found.
   *
   * @param assertionUrn the urn of the View
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  public EntityResponse getAssertionEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.ASSERTION_ENTITY_NAME,
          assertionUrn,
          ImmutableSet.of(
              Constants.ASSERTION_INFO_ASPECT_NAME,
              Constants.ASSERTION_ACTIONS_ASPECT_NAME,
              Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Creates a new Dataset or DataJob Freshness Assertion for native execution by DataHub. Assumes
   * that the caller has already performed the required authorization.
   */
  @Nonnull
  public Urn createFreshnessAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final FreshnessAssertionType type,
      @Nonnull final FreshnessAssertionSchedule schedule,
      @Nullable final DatasetFilter filter,
      @Nullable final AssertionActions actions) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(opContext, "authentication must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final Urn assertionUrn = generateAssertionUrn();

    final FreshnessAssertionInfo freshnessInfo =
        new FreshnessAssertionInfo()
            .setEntity(entityUrn)
            .setType(type)
            .setSchedule(schedule, SetMode.IGNORE_NULL)
            .setFilter(filter, SetMode.IGNORE_NULL);

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setFreshnessAssertion(freshnessInfo);
    assertion.setType(AssertionType.FRESHNESS);
    assertion.setSource(getNativeAssertionSource(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to create new Freshness Assertion for entity with urn %s", entityUrn),
          e);
    }
  }

  /**
   * Creates a new Volume Assertion for native execution by DataHub. Assumes that the caller has
   * already performed the required authorization.
   */
  @Nonnull
  public Urn createVolumeAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final VolumeAssertionType type,
      @Nonnull final VolumeAssertionInfo info,
      @Nullable final AssertionActions actions) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(info, "info must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    switch (type) {
      case ROW_COUNT_TOTAL:
        Objects.requireNonNull(info.getRowCountTotal(), "rowCountTotal must not be null");
        break;
      case ROW_COUNT_CHANGE:
        Objects.requireNonNull(info.getRowCountChange(), "rowCountChange must not be null");
        break;
      case INCREMENTING_SEGMENT_ROW_COUNT_TOTAL:
        Objects.requireNonNull(
            info.getIncrementingSegmentRowCountTotal(),
            "incrementingSegmentRowCountTotal must not be null");
        break;
      case INCREMENTING_SEGMENT_ROW_COUNT_CHANGE:
        Objects.requireNonNull(
            info.getIncrementingSegmentRowCountChange(),
            "incrementingSegmentRowCountChange must not be null");
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Failed to create Volume Assertion. Unsupported VolumeAssertionType %s", type));
    }

    final Urn assertionUrn = generateAssertionUrn();

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setVolumeAssertion(info);
    assertion.setType(AssertionType.VOLUME);
    assertion.setSource(getNativeAssertionSource(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create new Volume Assertion for entity with urn %s", entityUrn),
          e);
    }
  }

  /**
   * Creates a new SQL Assertion for native execution by DataHub. Assumes that the caller has
   * already performed the required authorization.
   */
  @Nonnull
  public Urn createSqlAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final SqlAssertionType type,
      @Nonnull final String description,
      @Nonnull final SqlAssertionInfo info,
      @Nullable final AssertionActions actions) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(description, "description must not be null");
    Objects.requireNonNull(info, "info must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final Urn assertionUrn = generateAssertionUrn();

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setSqlAssertion(info);
    assertion.setType(AssertionType.SQL);
    assertion.setDescription(description);
    assertion.setSource(getNativeAssertionSource(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create new SQL Assertion for entity with urn %s", entityUrn), e);
    }
  }

  /**
   * Creates a new Field Assertion for native execution by DataHub. Assumes that the caller has
   * already performed the required authorization.
   */
  @Nonnull
  public Urn createFieldAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final FieldAssertionInfo info,
      @Nullable final AssertionActions actions) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(info, "info must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final Urn assertionUrn = generateAssertionUrn();

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setFieldAssertion(info);
    assertion.setType(AssertionType.FIELD);
    assertion.setSource(getNativeAssertionSource(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create new Field Assertion for entity with urn %s", entityUrn),
          e);
    }
  }

  /**
   * Creates a new Dataset Metrics Assertion for native execution by DataHub. Assumes that the
   * caller has already performed the required authorization.
   */
  @Nonnull
  public Urn createDatasetAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn datasetUrn,
      @Nonnull final DatasetAssertionScope scope,
      @Nullable final List<Urn> fields,
      @Nullable final AssertionStdAggregation aggregation,
      @Nonnull final AssertionStdOperator operator,
      @Nullable final AssertionStdParameters parameters,
      @Nullable final AssertionActions actions) {
    Objects.requireNonNull(datasetUrn, "datasetUrn must not be null");
    Objects.requireNonNull(scope, "scope must not be null");
    Objects.requireNonNull(operator, "operator must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final Urn assertionUrn = generateAssertionUrn();

    final DatasetAssertionInfo datasetAssertion =
        new DatasetAssertionInfo()
            .setDataset(datasetUrn)
            .setScope(scope)
            .setFields(fields != null ? new UrnArray(fields) : null, SetMode.IGNORE_NULL)
            .setAggregation(aggregation, SetMode.IGNORE_NULL)
            .setOperator(operator)
            .setParameters(parameters, SetMode.IGNORE_NULL);

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setDatasetAssertion(datasetAssertion);
    assertion.setType(AssertionType.DATASET);
    assertion.setSource(getNativeAssertionSource(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to create new Dataset Assertion for entity with urn %s", assertionUrn),
          e);
    }
  }

  /**
   * Updates an existing Dataset Freshness Assertion for native execution by DataHub. Assumes that
   * the caller has already performed the required authorization.
   */
  @Nonnull
  public Urn upsertDatasetFreshnessAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Urn entityUrn,
      @Nullable final String description,
      @Nonnull final FreshnessAssertionSchedule schedule,
      @Nullable final DatasetFilter filter,
      @Nullable final AssertionActions actions,
      @Nullable final AssertionSource assertionSource) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(opContext, "authentication must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final FreshnessAssertionInfo freshnessInfo =
        new FreshnessAssertionInfo()
            .setEntity(entityUrn)
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setSchedule(schedule, SetMode.IGNORE_NULL)
            .setFilter(filter, SetMode.IGNORE_NULL);

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setFreshnessAssertion(freshnessInfo);
    assertion.setType(AssertionType.FRESHNESS);
    assertion.setSource(
        assertionSource != null ? assertionSource : getNativeAssertionSource(actorUrn));
    assertion.setDescription(description, SetMode.IGNORE_NULL);
    assertion.setLastUpdated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update Freshness Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Updates an existing Dataset Volume Assertion for native execution by DataHub. Assumes that the
   * caller has already performed the required authorization.
   */
  @Nonnull
  public Urn upsertDatasetVolumeAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Urn entityUrn,
      @Nullable final String description,
      @Nonnull final VolumeAssertionInfo info,
      @Nullable final AssertionActions actions,
      @Nullable final AssertionSource assertionSource) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(info.getType(), "type must not be null");
    Objects.requireNonNull(info, "info must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    switch (info.getType()) {
      case ROW_COUNT_TOTAL:
        Objects.requireNonNull(info.getRowCountTotal(), "rowCountTotal must not be null");
        break;
      case ROW_COUNT_CHANGE:
        Objects.requireNonNull(info.getRowCountChange(), "rowCountChange must not be null");
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Failed to create Volume Assertion. Unsupported VolumeAssertionType %s",
                info.getType()));
    }

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setVolumeAssertion(info);
    assertion.setType(AssertionType.VOLUME);
    assertion.setSource(
        assertionSource != null ? assertionSource : getNativeAssertionSource(actorUrn));
    assertion.setDescription(description, SetMode.IGNORE_NULL);
    assertion.setLastUpdated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update Volume Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Updates an existing Dataset Sql Assertion for native execution by DataHub. Assumes that the
   * caller has already performed the required authorization.
   */
  @Nonnull
  public Urn upsertDatasetSqlAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final SqlAssertionType type,
      @Nonnull final String description,
      @Nonnull final SqlAssertionInfo info,
      @Nullable final AssertionActions actions,
      @Nullable final AssertionSource assertionSource) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(description, "description must not be null");
    Objects.requireNonNull(info, "info must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setSqlAssertion(info);
    assertion.setType(AssertionType.SQL);
    assertion.setDescription(description);
    assertion.setSource(
        assertionSource != null ? assertionSource : getNativeAssertionSource(actorUrn));
    assertion.setLastUpdated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert new SQL Assertion for entity with urn %s", entityUrn), e);
    }
  }

  /**
   * Updates an existing Dataset Field Assertion for native execution by DataHub. Assumes that the
   * caller has already performed the required authorization.
   */
  @Nonnull
  public Urn upsertDatasetFieldAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Urn entityUrn,
      @Nullable final String description,
      @Nonnull final FieldAssertionInfo info,
      @Nullable final AssertionActions actions,
      @Nullable final AssertionSource assertionSource) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(info, "info must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setFieldAssertion(info);
    assertion.setType(AssertionType.FIELD);
    assertion.setSource(
        assertionSource != null ? assertionSource : getNativeAssertionSource(actorUrn));
    assertion.setDescription(description, SetMode.IGNORE_NULL);
    assertion.setLastUpdated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create new Field Assertion for entity with urn %s", entityUrn),
          e);
    }
  }

  /** Updates an existing Dataset Schema assertion if it exists, otherwise creates a new one. */
  @Nonnull
  public Urn upsertDatasetSchemaAssertion(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Urn entityUrn,
      @Nullable final String description,
      @Nonnull final SchemaAssertionCompatibility compatibility,
      @Nonnull final SchemaMetadata schema,
      @Nullable final AssertionActions actions,
      @Nullable final AssertionSource assertionSource) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(compatibility, "compatibility must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    final SchemaAssertionInfo schemaAssertionInfo =
        new SchemaAssertionInfo()
            .setEntity(entityUrn)
            .setCompatibility(compatibility)
            .setSchema(schema);

    final AssertionInfo assertion = new AssertionInfo();
    assertion.setSchemaAssertion(schemaAssertionInfo);
    assertion.setType(AssertionType.DATA_SCHEMA);
    assertion.setSource(
        assertionSource != null ? assertionSource : getNativeAssertionSource(actorUrn));
    assertion.setDescription(description, SetMode.IGNORE_NULL);
    assertion.setLastUpdated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertion));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update Schema Assertion with urn %s", assertionUrn), e);
    }
  }

  /** Updates basic metadata for a given assertion such as description and actions executed. */
  @Nonnull
  public Urn updateAssertionMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nullable final AssertionActions actions,
      @Nullable final String assertionDescription) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    // 1. Check whether the Assertion exists
    AssertionInfo existingInfo = getAssertionInfo(opContext, assertionUrn);

    if (existingInfo == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    // 2. Ingest actions aspect
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }
    // 3. Ingest assertion info aspect changes
    if (assertionDescription != null) {
      existingInfo.setDescription(assertionDescription);
    }
    existingInfo.setLastUpdated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, existingInfo));

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update actions for Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Updates an existing Dataset Metrics Assertion for native execution by DataHub. Assumes that the
   * caller has already performed the required authorization.
   */
  @Nonnull
  public Urn updateDatasetAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final DatasetAssertionScope scope,
      @Nullable final List<Urn> fields,
      @Nullable final AssertionStdAggregation aggregation,
      @Nonnull final AssertionStdOperator operator,
      @Nullable final AssertionStdParameters parameters,
      @Nullable final AssertionActions actions) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(scope, "scope must not be null");
    Objects.requireNonNull(operator, "operator must not be null");
    Objects.requireNonNull(operator, "opContext must not be null");
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    Objects.requireNonNull(actorUrn, "actorUrn obtained through authentication must not be null");

    // 1. Check whether the Assertion exists
    AssertionInfo existingInfo = getAssertionInfo(opContext, assertionUrn);

    if (existingInfo == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }

    if (!AssertionType.DATASET.equals(existingInfo.getType())) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s is not an Dataset assertion.",
              assertionUrn));
    }

    if (!existingInfo.hasDatasetAssertion()) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Dataset Assertion with urn %s is malformed!",
              assertionUrn));
    }

    DatasetAssertionInfo existingDatasetAssertion = existingInfo.getDatasetAssertion();

    // 2. Apply changes to existing Assertion Info
    existingDatasetAssertion.setScope(scope);
    existingDatasetAssertion.setFields(
        fields != null ? new UrnArray(fields) : null, SetMode.IGNORE_NULL);
    existingDatasetAssertion.setAggregation(aggregation, SetMode.IGNORE_NULL);
    existingDatasetAssertion.setOperator(operator);
    existingDatasetAssertion.setParameters(parameters, SetMode.IGNORE_NULL);

    existingInfo.setLastUpdated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, existingInfo));
    if (actions != null) {
      aspects.add(
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_ACTIONS_ASPECT_NAME, actions));
    }

    // 3. Ingest updates aspects
    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update Dataset Assertion with urn %s", assertionUrn), e);
    }
  }

  @Nonnull
  private AssertionSource getNativeAssertionSource(Urn actorUrn) {
    final AssertionSource source = new AssertionSource();
    source.setType(AssertionSourceType.NATIVE);
    source.setCreated(new AuditStamp().setTime(getCurrentTime()).setActor(actorUrn));
    return source;
  }

  @Nonnull
  public Urn generateAssertionUrn() {
    final AssertionKey key = new AssertionKey();
    final String id = UUID.randomUUID().toString();
    key.setAssertionId(id);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.ASSERTION_ENTITY_NAME);
  }

  public void tryDeleteAssertion(@Nonnull OperationContext opContext, Urn assertionUrn) {
    try {
      entityClient.deleteEntity(opContext, assertionUrn);
    } catch (RemoteInvocationException ex) {
      log.error(String.format("Failed to delete assertion with urn %s ", assertionUrn), ex);
    }
  }

  public void tryDeleteAssertionReferences(@Nonnull OperationContext opContext, Urn assertionUrn) {
    try {
      entityClient.deleteEntityReferences(opContext, assertionUrn);
    } catch (RemoteInvocationException ex) {
      log.error(
          String.format("Failed to delete assertion references for urn %s! ", assertionUrn), ex);
    }
  }

  private long getCurrentTime() {
    return _clock.millis();
  }
}
