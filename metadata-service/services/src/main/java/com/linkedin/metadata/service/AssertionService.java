package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
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
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.patch.builder.AssertionRunSummaryPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.AssertionsSummaryPatchBuilder;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertionService extends BaseService {

  /**
   * Maximum number of entities to list when searching for entities with a given assertion summary.
   * This can be low since assertions are usually tied to 1 entity.
   */
  static final int MAX_ENTITIES_TO_LIST = 5000;

  static final String FAILING_ASSERTIONS_INDEX_FIELD_NAME = "failingAssertions";
  static final String PASSING_ASSERTIONS_INDEX_FIELD_NAME = "passingAssertions";

  static final List<String> ENTITY_TYPES_WITH_ASSERTION_SUMMARIES =
      ImmutableList.of(Constants.DATASET_ENTITY_NAME);
  private static final String ASSERTS_RELATIONSHIP_NAME = "Asserts";
  private static final String MONITOR_EVALUATES_RELATIONSHIP_NAME = "Evaluates";

  private static final int MAX_ASSERTIONS_TO_LIST = 1000;
  private final GraphClient _graphClient;
  private final Clock _clock;

  public AssertionService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull final Clock clock,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
    _graphClient = graphClient;
    _clock = clock;
  }

  public AssertionService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
    _graphClient = graphClient;
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
   * Returns an instance of {@link AssertionsSummary} for the specified Entity urn, or null if one
   * cannot be found.
   *
   * @param entityUrn the urn of the entity to retrieve the summary for
   * @return an instance of {@link AssertionsSummary} for the Entity, null if it does not exist.
   */
  @Nullable
  public AssertionsSummary getAssertionsSummary(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    final EntityResponse response = getAssertionsSummaryResponse(opContext, entityUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)) {
      return new AssertionsSummary(
          response.getAspects().get(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link AssertionRunSummary} for the specified asseriton urn, or null if
   * one cannot be found.
   *
   * @param assertionUrn the urn of the assertion to retrieve the summary for
   * @return an instance of {@link AssertionRunSummary} for the assertion, null if it does not
   *     exist.
   */
  @Nullable
  public AssertionRunSummary getAssertionRunSummary(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    final EntityResponse response = getAssertionEntityResponse(opContext, assertionUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME)) {
      return new AssertionRunSummary(
          response.getAspects().get(Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME).getValue().data());
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
   * Retrieves the list of assertions associated with the target entity.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the entity to retrieve assertions for
   * @return a list of assertion urns associated with the entity
   */
  public List<Urn> getAssertionUrnsForEntity(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      // Fetch set of assertions associated with the target entity from the Graph
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              entityUrn.toString(),
              ImmutableSet.of(ASSERTS_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              0,
              MAX_ASSERTIONS_TO_LIST,
              opContext.getActorContext().getActorUrn().toString());

      final List<Urn> assertionUrns =
          relationships.getRelationships().stream()
              .map(EntityRelationship::getEntity)
              .collect(Collectors.toList());

      // Filter out assertions that exist (not hard or soft deleted)
      return assertionUrns.stream()
          .filter(urn -> assertionExists(urn, false, opContext))
          .collect(Collectors.toList());

    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve assertions for entity with urn %s", entityUrn), e);
    }
  }

  /**
   * Retrieves the entity associated with the assertion
   *
   * @param opContext the operation context
   * @param assertionUrn the urn of the assertion to retrieve entity for
   * @return Entity urn associated with the assertion
   */
  public @Nullable Urn getEntityUrnForAssertion(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    try {
      // Fetch the entity associated with the assertion from the Graph
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              assertionUrn.toString(),
              ImmutableSet.of(ASSERTS_RELATIONSHIP_NAME),
              RelationshipDirection.OUTGOING,
              0,
              1,
              opContext.getActorContext().getActorUrn().toString());

      if (relationships.hasRelationships() && !relationships.getRelationships().isEmpty()) {
        return relationships.getRelationships().get(0).getEntity();
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve entity for assertion with urn %s", assertionUrn), e);
    }
    return null;
  }

  /**
   * Retrieves the monitor associated with the assertion
   *
   * @param opContext the operation context
   * @param assertionUrn the urn of the assertion to retrieve monitor for
   * @return Entity urn associated with the assertion
   */
  public @Nullable Urn getMonitorUrnForAssertion(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    try {
      // Fetch the entity associated with the assertion from the Graph
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              assertionUrn.toString(),
              ImmutableSet.of(MONITOR_EVALUATES_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              0,
              1,
              opContext.getActorContext().getActorUrn().toString());

      if (relationships.hasRelationships() && !relationships.getRelationships().isEmpty()) {
        return relationships.getRelationships().get(0).getEntity();
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve monitor for assertion with urn %s", assertionUrn), e);
    }
    return null;
  }

  /**
   * Produces a Metadata Change Proposal to update the AssertionsSummary aspect for a given entity.
   */
  public void updateAssertionsSummary(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final AssertionsSummary newSummary)
      throws Exception {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(newSummary, "newSummary must not be null");
    this.entityClient.ingestProposal(
        opContext,
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, Constants.ASSERTIONS_SUMMARY_ASPECT_NAME, newSummary),
        false);
  }

  /** Patches an assertions summary aspect */
  public void patchAssertionsSummary(
      @Nonnull final OperationContext opContext,
      @Nonnull final AssertionsSummaryPatchBuilder patchBuilder)
      throws Exception {
    Objects.requireNonNull(patchBuilder, "patchBuilder must not be null");
    this.entityClient.ingestProposal(opContext, patchBuilder.build(), false);
  }

  /** Patches an assertion run summary aspect */
  public void patchAssertionRunSummary(
      @Nonnull final OperationContext opContext,
      @Nonnull final AssertionRunSummaryPatchBuilder patchBuilder)
      throws Exception {
    Objects.requireNonNull(patchBuilder, "patchBuilder must not be null");
    this.entityClient.ingestProposal(opContext, patchBuilder.build(), false);
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
              Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
              Constants.GLOBAL_TAGS_ASPECT_NAME,
              Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified Entity urn containing the
   * assertions summary aspect or null if one cannot be found.
   *
   * @param entityUrn the urn of the Entity for which to fetch assertion summary
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  private EntityResponse getAssertionsSummaryResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Assertion Summary for entity with urn %s", entityUrn),
          e);
    }
  }

  /**
   * Finds all the entity urns that have the specified assertion URN in their summary currently.
   * This is done by using the search API to filter on assets with a given summary urn.
   *
   * <p>Returns a list of entity urns that have the specified assertion URN in their summary, or
   * empty list if none are found!
   */
  @Nonnull
  public List<Urn> listEntitiesWithAssertionInSummary(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      SearchResult result =
          this.entityClient.searchAcrossEntities(
              opContext,
              ENTITY_TYPES_WITH_ASSERTION_SUMMARIES,
              "*",
              buildFilterForAssertionSummary(assertionUrn),
              0,
              MAX_ENTITIES_TO_LIST,
              Collections.emptyList(),
              null);
      if (result == null || !result.hasEntities()) {
        return Collections.emptyList();
      }
      return result.getEntities().stream()
          .map(SearchEntity::getEntity)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to list entities with assertion summary containing assertion urn %s",
              assertionUrn),
          e);
    }
  }

  /**
   * Returns an instance of {@link com.linkedin.assertion.AssertionResult} for the specified
   * assertion URN, if one exists, null if one is not found.
   */
  @Nullable
  public AssertionResult getLatestAssertionRunResult(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    final AssertionRunEvent event = getLatestAssertionRunEvent(opContext, assertionUrn);
    return event != null ? event.getResult() : null;
  }

  /**
   * Returns an instance of {@link com.linkedin.assertion.AssertionRunEvent} for the specified
   * assertion URN, at the given timestamp, if one exists.
   */
  @Nullable
  public AssertionRunEvent getAssertionRunEvent(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final long timestampMillis) {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(timestampMillis, "timestampMillis must not be null");
    try {
      final List<EnvelopedAspect> aspects =
          this.entityClient.getTimeseriesAspectValues(
              opContext,
              assertionUrn.toString(),
              Constants.ASSERTION_ENTITY_NAME,
              Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
              timestampMillis,
              null,
              1,
              null);
      if (aspects != null && !aspects.isEmpty()) {
        final EnvelopedAspect envelopedAspect = aspects.get(0);
        return GenericRecordUtils.deserializeAspect(
            envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(),
            AssertionRunEvent.class);
      }
      return null;
    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Failed to retrieve Assertion Run Events from GMS", e);
    }
  }

  /**
   * Returns an instance of {@link com.linkedin.assertion.AssertionRunEvent} for the specified
   * assertion URN, if one exists, null if one is not found.
   */
  @Nullable
  public AssertionRunEvent getLatestAssertionRunEvent(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    try {
      final List<EnvelopedAspect> aspects =
          this.entityClient.getTimeseriesAspectValues(
              opContext,
              assertionUrn.toString(),
              Constants.ASSERTION_ENTITY_NAME,
              Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
              null,
              null,
              1,
              null);
      if (aspects != null && !aspects.isEmpty()) {
        final EnvelopedAspect envelopedAspect = aspects.get(0);
        return GenericRecordUtils.deserializeAspect(
            envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(),
            AssertionRunEvent.class);
      }
      return null;
    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Failed to retrieve Assertion Run Events from GMS", e);
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
      @Nullable final FreshnessAssertionSchedule schedule,
      @Nullable final DatasetFilter filter,
      @Nullable final AssertionActions actions,
      @Nullable final AssertionSource assertionSource) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(opContext, "authentication must not be null");
    if (assertionSource == null || assertionSource.getType() != AssertionSourceType.INFERRED) {
      Objects.requireNonNull(schedule, "schedule must not be null for non-AI assertions");
    }
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

  private boolean assertionExists(
      @Nonnull Urn urn, @Nonnull Boolean includeSoftDeleted, @Nonnull OperationContext opContext) {
    try {
      return this.entityClient.exists(opContext, urn, includeSoftDeleted);
    } catch (RemoteInvocationException e) {
      log.error(String.format("Unable to check if assertion %s exists, ignoring it", urn), e);
      return false;
    }
  }

  private long getCurrentTime() {
    return _clock.millis();
  }

  @Nonnull
  private Filter buildFilterForAssertionSummary(@Nonnull final Urn assertionUrn) {
    /*
     * Filter for an OR between assets with the assertion in their
     * 1. Passing Details Array OR
     * 2. Failing Details Array
     */
    final List<ConjunctiveCriterion> orConditions =
        ImmutableList.of(
            buildEqualsCriterion(PASSING_ASSERTIONS_INDEX_FIELD_NAME, assertionUrn.toString()),
            buildEqualsCriterion(FAILING_ASSERTIONS_INDEX_FIELD_NAME, assertionUrn.toString()));
    return new Filter().setOr(new ConjunctiveCriterionArray(orConditions));
  }

  @Nonnull
  private ConjunctiveCriterion buildEqualsCriterion(String field, String value) {
    return new ConjunctiveCriterion()
        .setAnd(
            new CriterionArray(
                ImmutableList.of(CriterionUtils.buildCriterion(field, Condition.EQUAL, value))));
  }

  public Urn upsertCustomAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull Urn assertionUrn,
      @Nonnull Urn entityUrn,
      @Nonnull String description,
      @Nullable String externalUrl,
      @Nonnull DataPlatformInstance dataPlatformInstance,
      @Nonnull CustomAssertionInfo customAssertionInfo) {

    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(description, "description must not be null");
    Objects.requireNonNull(customAssertionInfo, "info must not be null");
    Objects.requireNonNull(dataPlatformInstance, "opContext must not be null");

    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.CUSTOM);
    assertionInfo.setDescription(description);
    if (externalUrl != null) {
      assertionInfo.setExternalUrl(new Url(externalUrl));
    }
    assertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    assertionInfo.setCustomAssertion(customAssertionInfo);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertionInfo));
    aspects.add(
        (AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME, dataPlatformInstance)));

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert Custom Assertion with urn %s", assertionUrn), e);
    }
  }

  public void addAssertionRunEvent(
      @Nonnull OperationContext opContext,
      @Nonnull Urn assertionUrn,
      @Nonnull Urn asserteeUrn,
      @Nonnull Long timestampMillis,
      @Nonnull AssertionResult assertionResult) {
    AssertionRunEvent assertionRunEvent = new AssertionRunEvent();
    assertionRunEvent.setTimestampMillis(timestampMillis);
    assertionRunEvent.setRunId(timestampMillis.toString());
    assertionRunEvent.setAssertionUrn(assertionUrn);
    assertionRunEvent.setAsserteeUrn(asserteeUrn);
    assertionRunEvent.setStatus(AssertionRunStatus.COMPLETE);
    assertionRunEvent.setResult(assertionResult);

    try {
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildSynchronousMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_RUN_EVENT_ASPECT_NAME, assertionRunEvent),
          false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to upsert Assertion Run Event for assertion with urn %s", assertionUrn),
          e);
    }
  }
}
