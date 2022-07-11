package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ValidationException;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.MetadataChangeProposal;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.resources.restli.RestliConstants.*;


/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@Slf4j
@RestLiCollection(name = "aspects", namespace = "com.linkedin.entity")
public class AspectResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {

  private static final String ACTION_GET_TIMESERIES_ASPECT = "getTimeseriesAspectValues";
  private static final String ACTION_INGEST_PROPOSAL = "ingestProposal";

  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ASPECT = "aspect";
  private static final String PARAM_PROPOSAL = "proposal";
  private static final String PARAM_START_TIME_MILLIS = "startTimeMillis";
  private static final String PARAM_END_TIME_MILLIS = "endTimeMillis";
  private static final String PARAM_LATEST_VALUE = "latestValue";

  private final Clock _clock = Clock.systemUTC();

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   * TODO: Get rid of this and migrate to getAspect.
   */
  @RestMethod.Get
  @Nonnull
  @WithSpan
  public Task<AnyRecord> get(@Nonnull String urnStr, @QueryParam("aspect") @Optional @Nullable String aspectName,
      @QueryParam("version") @Optional @Nullable Long version) throws URISyntaxException {
    log.info("GET ASPECT urn: {} aspect: {} version: {}", urnStr, aspectName, version);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      final VersionedAspect aspect = _entityService.getVersionedAspect(urn, aspectName, version);
      if (aspect == null) {
        throw RestliUtil.resourceNotFoundException();
      }
      return new AnyRecord(aspect.data());
    }, MetricRegistry.name(this.getClass(), "get"));
  }

  @Action(name = ACTION_GET_TIMESERIES_ASPECT)
  @Nonnull
  @WithSpan
  public Task<GetTimeseriesAspectValuesResponse> getTimeseriesAspectValues(
      @ActionParam(PARAM_URN) @Nonnull String urnStr, @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_ASPECT) @Nonnull String aspectName,
      @ActionParam(PARAM_START_TIME_MILLIS) @Optional @Nullable Long startTimeMillis,
      @ActionParam(PARAM_END_TIME_MILLIS) @Optional @Nullable Long endTimeMillis,
      @ActionParam(PARAM_LIMIT) @Optional("10000") int limit,
      @ActionParam(PARAM_LATEST_VALUE) @Optional("false") boolean latestValue,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter) throws URISyntaxException {
    log.info(
        "Get Timeseries Aspect values for aspect {} for entity {} with startTimeMillis {}, endTimeMillis {} and limit {}.",
        aspectName, entityName, startTimeMillis, endTimeMillis, limit);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      GetTimeseriesAspectValuesResponse response = new GetTimeseriesAspectValuesResponse();
      response.setEntityName(entityName);
      response.setAspectName(aspectName);
      if (startTimeMillis != null) {
        response.setStartTimeMillis(startTimeMillis);
      }
      if (endTimeMillis != null) {
        response.setEndTimeMillis(endTimeMillis);
      }
      response.setLimit(limit);
      response.setValues(new EnvelopedAspectArray(
          _timeseriesAspectService.getAspectValues(urn, entityName, aspectName, startTimeMillis, endTimeMillis, limit,
              latestValue, filter)));
      return response;
    }, MetricRegistry.name(this.getClass(), "getTimeseriesAspectValues"));
  }

  @Action(name = ACTION_INGEST_PROPOSAL)
  @Nonnull
  @WithSpan
  public Task<String> ingestProposal(
      @ActionParam(PARAM_PROPOSAL) @Nonnull MetadataChangeProposal metadataChangeProposal) throws URISyntaxException {
    log.info("INGEST PROPOSAL proposal: {}", metadataChangeProposal);

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    final AuditStamp auditStamp = new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(actorUrnStr));

    final List<MetadataChangeProposal> additionalChanges =
        AspectUtils.getAdditionalChanges(metadataChangeProposal, _entityService);

    return RestliUtil.toTask(() -> {
      log.debug("Proposal: {}", metadataChangeProposal);
      try {
        Urn urn = _entityService.ingestProposal(metadataChangeProposal, auditStamp).getUrn();
        additionalChanges.forEach(proposal -> _entityService.ingestProposal(proposal, auditStamp));
        return urn.toString();
      } catch (ValidationException e) {
        throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, e.getMessage());
      }
    }, MetricRegistry.name(this.getClass(), "ingestProposal"));
  }

}
