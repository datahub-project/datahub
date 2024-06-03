package com.linkedin.metadata.resources.entity;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.datahub.authorization.AuthUtil.isAPIAuthorizedEntityUrns;
import static com.datahub.authorization.AuthUtil.isAPIAuthorizedUrns;
import static com.linkedin.metadata.authorization.ApiGroup.COUNTS;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiGroup.TIMESERIES;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.resources.operations.OperationsResource.*;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.authorization.Disjunctive;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.resources.operations.Utils;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
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
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
  private EntityService<?> _entityService;

  @VisibleForTesting
  void setEntityService(EntityService<?> entityService) {
    _entityService = entityService;
  }

  @Inject
  @Named("entitySearchService")
  private EntitySearchService entitySearchService;

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

    @Inject
    @Named("systemOperationContext")
    private OperationContext systemOperationContext;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

  @VisibleForTesting
  void setAuthorizer(Authorizer authorizer) {
    _authorizer = authorizer;
  }

  @VisibleForTesting
  void setSystemOperationContext(OperationContext systemOperationContext) {
      this.systemOperationContext = systemOperationContext;
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

            Authentication auth = AuthenticationContext.getAuthentication();
          if (!isAPIAuthorizedEntityUrns(
                  auth,
                  _authorizer,
                  READ,
                  List.of(urn))) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get aspect for " + urn);
          }
            final OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli("authorizerChain", urn.getEntityType()), _authorizer, auth, true);

          final VersionedAspect aspect =
              _entityService.getVersionedAspect(opContext, urn, aspectName, version);
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

            Authentication auth = AuthenticationContext.getAuthentication();
          if (!isAPIAuthorizedUrns(
                  auth,
                  _authorizer,
                  TIMESERIES, READ,
                  List.of(urn))) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN,
                "User is unauthorized to get timeseries aspect for " + urn);
          }
            final OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(ACTION_GET_TIMESERIES_ASPECT, urn.getEntityType()), _authorizer, auth, true);

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
                  timeseriesAspectService.getAspectValues(opContext,
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
      final OperationContext opContext = OperationContext.asSession(
              systemOperationContext, RequestContext.builder().buildRestli(ACTION_INGEST_PROPOSAL, metadataChangeProposal.getEntityType()), _authorizer, authentication, true);

    /*
      Ingest Authorization Checks
     */
     List<Pair<MetadataChangeProposal, Integer>> exceptions = isAPIAuthorized(authentication, _authorizer, ENTITY,
             opContext.getEntityRegistry(), List.of(metadataChangeProposal))
             .stream().filter(p -> p.getSecond() != HttpStatus.S_200_OK.getCode())
             .collect(Collectors.toList());
     if (!exceptions.isEmpty()) {
         throw new RestLiServiceException(
                 HttpStatus.S_403_FORBIDDEN, "User is unauthorized to modify entity: " + exceptions.stream()
                 .map(ex -> String.format("HttpStatus: %s Urn: %s", ex.getSecond(), ex.getFirst().getEntityUrn()))
                 .collect(Collectors.toList()));
     }

    String actorUrnStr = authentication.getActor().toUrnStr();
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(actorUrnStr));

    return RestliUtil.toTask(() -> {
      log.debug("Proposal: {}", metadataChangeProposal);
      try {
        final AspectsBatch batch = AspectsBatchImpl.builder()
                .mcps(List.of(metadataChangeProposal), auditStamp, opContext.getRetrieverContext().get())
                .build();

        Set<IngestResult> results =
                _entityService.ingestProposal(opContext, batch, asyncBool);

            java.util.Optional<IngestResult> one = results.stream().findFirst();

            // Update runIds, only works for existing documents, so ES document must exist
            Urn resultUrn = one.map(IngestResult::getUrn).orElse(metadataChangeProposal.getEntityUrn());
            if (one.map(result -> result.isProcessedMCL() || result.isUpdate()).orElse(false)) {
              tryIndexRunId(opContext,
                  resultUrn, metadataChangeProposal.getSystemMetadata(), entitySearchService);
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
          if (!isAPIAuthorized(
                  authentication,
                  _authorizer,
                  COUNTS, READ)) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get aspect counts.");
          }
            final OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(ACTION_GET_COUNT, List.of()), _authorizer, authentication, true);

            return _entityService.getCountAspect(opContext, aspectName, urnLike);
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
      @ActionParam("batchSize") @Optional @Nullable Integer batchSize,
      @ActionParam("limit") @Optional @Nullable Integer limit,
      @ActionParam("gePitEpochMs") @Optional @Nullable Long gePitEpochMs,
      @ActionParam("lePitEpochMs") @Optional @Nullable Long lePitEpochMs) {
    return RestliUtil.toTask(
        () -> {
            if (!isAPIAuthorized(
                    AuthenticationContext.getAuthentication(),
                    _authorizer,
                    PoliciesConfig.RESTORE_INDICES_PRIVILEGE)) {
                throw new RestLiServiceException(
                        HttpStatus.S_403_FORBIDDEN, "User is unauthorized to update entities.");
            }

            return Utils.restoreIndices(systemOperationContext,
              aspectName, urn, urnLike, start, batchSize, limit, gePitEpochMs, lePitEpochMs, _authorizer, _entityService);
        },
        MetricRegistry.name(this.getClass(), "restoreIndices"));
  }

  private static void tryIndexRunId(
          @Nonnull final OperationContext opContext,
      final Urn urn,
      final @Nullable SystemMetadata systemMetadata,
      final EntitySearchService entitySearchService) {
    if (systemMetadata != null && systemMetadata.hasRunId()) {
      entitySearchService.appendRunId(opContext, urn.getEntityType(), urn, systemMetadata.getRunId());
    }
  }
}
