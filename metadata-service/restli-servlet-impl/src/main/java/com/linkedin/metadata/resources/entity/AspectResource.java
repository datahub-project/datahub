package com.linkedin.metadata.resources.entity;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.resources.operations.OperationsResource.*;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.resources.restli.RestliUtils.*;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.resources.operations.Utils;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.internal.server.methods.AnyRecord;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/** Single unified resource for fetching, updating, searching, & browsing DataHub entities */
@Slf4j
@RestLiCollection(name = "aspects", namespace = "com.linkedin.entity")
public class AspectResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {

  private static final String ACTION_GET_TIMESERIES_ASPECT = "getTimeseriesAspectValues";
  private static final String ACTION_INGEST_PROPOSAL = "ingestProposal";
  private static final String ACTION_GET_COUNT = "getCount";
  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ASPECT = "aspect";
  private static final String PARAM_PROPOSAL = "proposal";
  private static final String PARAM_START_TIME_MILLIS = "startTimeMillis";
  private static final String PARAM_END_TIME_MILLIS = "endTimeMillis";
  private static final String PARAM_LATEST_VALUE = "latestValue";
  private static final String PARAM_ASYNC = "async";

  private static final String ASYNC_INGEST_DEFAULT_NAME = "ASYNC_INGEST_DEFAULT";
  private static final String UNSET = "unset";

  private final Clock _clock = Clock.systemUTC();

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @VisibleForTesting
  void setEntityService(EntityService entityService) {
    _entityService = entityService;
  }

  @Inject
  @Named("entitySearchService")
  private EntitySearchService _entitySearchService;

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

  @VisibleForTesting
  void setAuthorizer(Authorizer authorizer) {
    _authorizer = authorizer;
  }

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   * TODO: Get rid of this and migrate to getAspect.
   */
  @RestMethod.Get
  @Nonnull
  @WithSpan
  public Task<AnyRecord> get(
      @Nonnull String urnStr,
      @QueryParam("aspect") @Optional @Nullable String aspectName,
      @QueryParam("version") @Optional @Nullable Long version)
      throws URISyntaxException {
    log.info("GET ASPECT urn: {} aspect: {} version: {}", urnStr, aspectName, version);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(
        () -> {
          Authentication authentication = AuthenticationContext.getAuthentication();
          if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
              && !isAuthorized(
                  authentication,
                  _authorizer,
                  ImmutableList.of(PoliciesConfig.GET_ENTITY_PRIVILEGE),
                  new EntitySpec(urn.getEntityType(), urn.toString()))) {
            throw new RestLiServiceException(
                HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to get aspect for " + urn);
          }
          final VersionedAspect aspect =
              _entityService.getVersionedAspect(urn, aspectName, version);
          if (aspect == null) {
            throw RestliUtil.resourceNotFoundException(
                String.format(
                    "Did not find urn: %s aspect: %s version: %s", urn, aspectName, version));
          }
          return new AnyRecord(aspect.data());
        },
        MetricRegistry.name(this.getClass(), "get"));
  }

  @Action(name = ACTION_GET_TIMESERIES_ASPECT)
  @Nonnull
  @WithSpan
  public Task<GetTimeseriesAspectValuesResponse> getTimeseriesAspectValues(
      @ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_ASPECT) @Nonnull String aspectName,
      @ActionParam(PARAM_START_TIME_MILLIS) @Optional @Nullable Long startTimeMillis,
      @ActionParam(PARAM_END_TIME_MILLIS) @Optional @Nullable Long endTimeMillis,
      @ActionParam(PARAM_LIMIT) @Optional("10000") int limit,
      @ActionParam(PARAM_LATEST_VALUE) @Optional("false")
          boolean latestValue, // This field is deprecated.
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sort)
      throws URISyntaxException {
    log.info(
        "Get Timeseries Aspect values for aspect {} for entity {} with startTimeMillis {}, endTimeMillis {} and limit {}.",
        aspectName,
        entityName,
        startTimeMillis,
        endTimeMillis,
        limit);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(
        () -> {
          Authentication authentication = AuthenticationContext.getAuthentication();
          if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
              && !isAuthorized(
                  authentication,
                  _authorizer,
                  ImmutableList.of(PoliciesConfig.GET_TIMESERIES_ASPECT_PRIVILEGE),
                  new EntitySpec(urn.getEntityType(), urn.toString()))) {
            throw new RestLiServiceException(
                HttpStatus.S_401_UNAUTHORIZED,
                "User is unauthorized to get timeseries aspect for " + urn);
          }
          GetTimeseriesAspectValuesResponse response = new GetTimeseriesAspectValuesResponse();
          response.setEntityName(entityName);
          response.setAspectName(aspectName);
          if (startTimeMillis != null) {
            response.setStartTimeMillis(startTimeMillis);
          }
          if (endTimeMillis != null) {
            response.setEndTimeMillis(endTimeMillis);
          }
          if (latestValue) {
            response.setLimit(1);
          } else {
            response.setLimit(limit);
          }
          response.setValues(
              new EnvelopedAspectArray(
                  _timeseriesAspectService.getAspectValues(
                      urn,
                      entityName,
                      aspectName,
                      startTimeMillis,
                      endTimeMillis,
                      limit,
                      filter,
                      sort)));
          return response;
        },
        MetricRegistry.name(this.getClass(), "getTimeseriesAspectValues"));
  }

  @Action(name = ACTION_INGEST_PROPOSAL)
  @Nonnull
  @WithSpan
  public Task<String> ingestProposal(
      @ActionParam(PARAM_PROPOSAL) @Nonnull MetadataChangeProposal metadataChangeProposal,
      @ActionParam(PARAM_ASYNC) @Optional(UNSET) String async)
      throws URISyntaxException {
    log.info("INGEST PROPOSAL proposal: {}", metadataChangeProposal);

    final boolean asyncBool;
    if (UNSET.equals(async)) {
      asyncBool = Boolean.parseBoolean(System.getenv(ASYNC_INGEST_DEFAULT_NAME));
    } else {
      asyncBool = Boolean.parseBoolean(async);
    }

    Authentication authentication = AuthenticationContext.getAuthentication();
    com.linkedin.metadata.models.EntitySpec entitySpec =
        _entityService.getEntityRegistry().getEntitySpec(metadataChangeProposal.getEntityType());
    Urn urn =
        EntityKeyUtils.getUrnFromProposal(metadataChangeProposal, entitySpec.getKeyAspectSpec());
    if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
        && !isAuthorized(
            authentication,
            _authorizer,
            ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE),
            new EntitySpec(urn.getEntityType(), urn.toString()))) {
      throw new RestLiServiceException(
          HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to modify entity " + urn);
    }
    String actorUrnStr = authentication.getActor().toUrnStr();
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(actorUrnStr));

    return RestliUtil.toTask(
        () -> {
          log.debug("Proposal: {}", metadataChangeProposal);
          try {
            final AspectsBatch batch;
            if (asyncBool) {
              // if async we'll expand the getAdditionalChanges later, no need to do this early
              batch =
                  AspectsBatchImpl.builder()
                      .mcps(List.of(metadataChangeProposal), _entityService.getEntityRegistry())
                      .build();
            } else {
              Stream<MetadataChangeProposal> proposalStream =
                  Stream.concat(
                      Stream.of(metadataChangeProposal),
                      AspectUtils.getAdditionalChanges(metadataChangeProposal, _entityService)
                          .stream());

              batch =
                  AspectsBatchImpl.builder()
                      .mcps(
                          proposalStream.collect(Collectors.toList()),
                          _entityService.getEntityRegistry())
                      .build();
            }

            Set<IngestResult> results = _entityService.ingestProposal(batch, auditStamp, asyncBool);

            IngestResult one = results.stream().findFirst().get();

            // Update runIds, only works for existing documents, so ES document must exist
            Urn resultUrn = one.getUrn();
            if (one.isProcessedMCL() || one.isUpdate()) {
              tryIndexRunId(
                  resultUrn, metadataChangeProposal.getSystemMetadata(), _entitySearchService);
            }
            return resultUrn.toString();
          } catch (ValidationException e) {
            throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, e.getMessage());
          }
        },
        MetricRegistry.name(this.getClass(), "ingestProposal"));
  }

  @Action(name = ACTION_GET_COUNT)
  @Nonnull
  @WithSpan
  public Task<Integer> getCount(
      @ActionParam(PARAM_ASPECT) @Nonnull String aspectName,
      @ActionParam(PARAM_URN_LIKE) @Optional @Nullable String urnLike) {
    return RestliUtil.toTask(
        () -> {
          Authentication authentication = AuthenticationContext.getAuthentication();
          if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
              && !isAuthorized(
                  authentication,
                  _authorizer,
                  ImmutableList.of(PoliciesConfig.GET_COUNTS_PRIVILEGE),
                  (EntitySpec) null)) {
            throw new RestLiServiceException(
                HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to get aspect counts.");
          }
          return _entityService.getCountAspect(aspectName, urnLike);
        },
        MetricRegistry.name(this.getClass(), "getCount"));
  }

  @Action(name = ACTION_RESTORE_INDICES)
  @Nonnull
  @WithSpan
  public Task<String> restoreIndices(
      @ActionParam(PARAM_ASPECT) @Optional @Nonnull String aspectName,
      @ActionParam(PARAM_URN) @Optional @Nullable String urn,
      @ActionParam(PARAM_URN_LIKE) @Optional @Nullable String urnLike,
      @ActionParam("start") @Optional @Nullable Integer start,
      @ActionParam("batchSize") @Optional @Nullable Integer batchSize) {
    return RestliUtil.toTask(
        () -> {
          return Utils.restoreIndices(
              aspectName, urn, urnLike, start, batchSize, _authorizer, _entityService);
        },
        MetricRegistry.name(this.getClass(), "restoreIndices"));
  }

  private static void tryIndexRunId(
      final Urn urn,
      final @Nullable SystemMetadata systemMetadata,
      final EntitySearchService entitySearchService) {
    if (systemMetadata != null && systemMetadata.hasRunId()) {
      entitySearchService.appendRunId(urn.getEntityType(), urn, systemMetadata.getRunId());
    }
  }
}
