package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.parseq.Task;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.resources.ResourceUtils.*;
import static com.linkedin.metadata.restli.RestliConstants.*;

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
  public Task<VersionedAspect> get(
      @Nonnull String urnStr,
      @QueryParam("aspect") @Optional @Nullable String aspectName,
      @QueryParam("version") @Optional @Nullable Long version) throws URISyntaxException {
    log.info("GET ASPECT urn: {} aspect: {} version: {}", urnStr, aspectName, version);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      final VersionedAspect aspect = _entityService.getVersionedAspect(urn, aspectName, version);
      if (aspect == null) {
        throw RestliUtil.resourceNotFoundException();
      } else {
        validateOrWarn(aspect);
      }
      return aspect;
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
      @ActionParam(PARAM_LIMIT) @Optional("10000") int limit) throws URISyntaxException {
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
          _timeseriesAspectService.getAspectValues(urn, entityName, aspectName, startTimeMillis, endTimeMillis,
              limit)));
      return response;
    }, MetricRegistry.name(this.getClass(), "getTimeseriesAspectValues"));
  }

  @Action(name = ACTION_INGEST_PROPOSAL)
  @Nonnull
  @WithSpan
  public Task<String> ingestProposal(@ActionParam(PARAM_PROPOSAL) @Nonnull MetadataChangeProposal metadataChangeProposal)
      throws URISyntaxException {
    log.info("INGEST PROPOSAL proposal: {}", metadataChangeProposal);

    // TODO: Use the actor present in the IC.
    final AuditStamp auditStamp = new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(
        Constants.UNKNOWN_ACTOR));
    final List<MetadataChangeProposal> additionalChanges = getAdditionalChanges(metadataChangeProposal);

    return RestliUtil.toTask(() -> {
      log.debug("Proposal: {}", metadataChangeProposal);
      Urn urn = _entityService.ingestProposal(metadataChangeProposal, auditStamp);
      additionalChanges.forEach(proposal -> _entityService.ingestProposal(proposal, auditStamp));
      return urn.toString();
    }, MetricRegistry.name(this.getClass(), "ingestProposal"));
  }

  private List<MetadataChangeProposal> getAdditionalChanges(@Nonnull MetadataChangeProposal metadataChangeProposal)
      throws URISyntaxException {
    // No additional changes for delete operation
    if (metadataChangeProposal.getChangeType() == ChangeType.DELETE) {
      return Collections.emptyList();
    }

    final List<MetadataChangeProposal> additionalChanges = new ArrayList<>();

    final Urn urn = EntityKeyUtils.getUrnFromProposal(
        metadataChangeProposal,
        _entityService.getKeyAspectSpec(metadataChangeProposal.getEntityType()));

    // TODO: Run these in parallel.

    // Insert Key Aspect
    final MetadataChangeProposal maybeKeyAspectProposal = getKeyAspectProposal(urn, metadataChangeProposal);
    if (maybeKeyAspectProposal != null) {
      additionalChanges.add(maybeKeyAspectProposal);
    }

    // Insert Browse Paths Aspect
    final MetadataChangeProposal maybeBrowsePathsProposal = getBrowsePathsProposal(urn, metadataChangeProposal);
    if (maybeBrowsePathsProposal != null) {
      additionalChanges.add(maybeBrowsePathsProposal);
    }

    return additionalChanges;
  }

  private MetadataChangeProposal getKeyAspectProposal(Urn urn, MetadataChangeProposal original) {
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
    final RecordTemplate keyAspect = _entityService.getAspect(urn, keyAspectSpec.getName(), ASPECT_LATEST_VERSION);
    if (keyAspect == null) {
      try {
        MetadataChangeProposal keyAspectProposal = original.copy();
        GenericAspect aspect = GenericAspectUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema()));
        keyAspectProposal.setAspect(aspect);
        keyAspectProposal.setAspectName(keyAspectSpec.getName());
        return keyAspectProposal;
      } catch (CloneNotSupportedException e) {
        log.error("Issue while generating additional proposals corresponding to the input proposal", e);
      }
    }
    return null;
  }

  private MetadataChangeProposal getBrowsePathsProposal(Urn urn, MetadataChangeProposal original) throws URISyntaxException {
    final String browsePathsAspectName = "browsePaths";
    final EntitySpec entitySpec = _entityService.getEntityRegistry().getEntitySpec(urn.getEntityType());
    if (entitySpec.hasAspect(browsePathsAspectName)) {
      final RecordTemplate browsePathAspect =
          _entityService.getAspect(urn, browsePathsAspectName, ASPECT_LATEST_VERSION);
      if (browsePathAspect == null) {
        try {
          MetadataChangeProposal browsePathProposal = original.copy();
          GenericAspect aspect = GenericAspectUtils.serializeAspect(BrowsePathUtils.buildBrowsePath(urn));
          browsePathProposal.setAspect(aspect);
          browsePathProposal.setAspectName(browsePathsAspectName);
          return browsePathProposal;
        } catch (CloneNotSupportedException e) {
          log.error("Issue while generating additional proposals corresponding to the input proposal", e);
        }
      }
    }
    return null;
  }
}
