package com.linkedin.metadata.resources.entity;

import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.util.GenericAspectUtils;
import com.linkedin.metadata.utils.mxe.EntityKeyUtils;
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

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";

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
  public Task<VersionedAspect> get(@Nonnull String urnStr, @QueryParam("aspect") @Optional @Nullable String aspectName,
      @QueryParam("version") @Optional @Nullable Long version) throws URISyntaxException {
    log.info("GET ASPECT urn: {} aspect: {} version: {}", urnStr, aspectName, version);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtils.toTask(() -> {
      final VersionedAspect aspect = _entityService.getVersionedAspect(urn, aspectName, version);
      if (aspect == null) {
        throw RestliUtils.resourceNotFoundException();
      }
      return aspect;
    });
  }

  @Action(name = ACTION_GET_TIMESERIES_ASPECT)
  @Nonnull
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
    return RestliUtils.toTask(() -> {
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
      response.setValues(
          new EnvelopedAspectArray(_timeseriesAspectService.getAspectValues(urn, entityName, aspectName, startTimeMillis, endTimeMillis, limit)));
      return response;
    });
  }

  @Action(name = ACTION_INGEST_PROPOSAL)
  @Nonnull
  public Task<Void> ingestProposal(@ActionParam(PARAM_PROPOSAL) @Nonnull MetadataChangeProposal metadataChangeProposal)
      throws URISyntaxException {
    log.info("INGEST PROPOSAL proposal: {}", metadataChangeProposal);
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(DEFAULT_ACTOR));

    final List<MetadataChangeProposal> additionalChanges = getAdditionalChanges(metadataChangeProposal);

    return RestliUtils.toTask(() -> {
      log.debug("Proposal: {}", metadataChangeProposal);
      _entityService.ingestProposal(metadataChangeProposal, auditStamp);
      additionalChanges.forEach(proposal -> _entityService.ingestProposal(proposal, auditStamp));
      return null;
    });
  }

  private List<MetadataChangeProposal> getAdditionalChanges(@Nonnull MetadataChangeProposal metadataChangeProposal)
      throws URISyntaxException {
    // No additional changes for delete operation
    if (metadataChangeProposal.getChangeType() == ChangeType.DELETE) {
      return Collections.emptyList();
    }
    List<MetadataChangeProposal> additionalChanges = new ArrayList<>();
    final Urn urn = EntityKeyUtils.getUrnFromProposal(metadataChangeProposal);
    final RecordTemplate browsePathAspect =
        _entityService.getAspect(urn, "browsePaths", EntityService.LATEST_ASPECT_VERSION);
    if (browsePathAspect == null) {
      try {
        MetadataChangeProposal browsePathProposal = metadataChangeProposal.copy();
        GenericAspect aspect = GenericAspectUtils.serializeAspect(BrowsePathUtils.buildBrowsePath(urn));
        browsePathProposal.setAspect(aspect);
        browsePathProposal.setAspectName("browsePaths");
        additionalChanges.add(browsePathProposal);
      } catch (CloneNotSupportedException e) {
        log.error("Issue while generating additional proposals corresponding to the input proposal", e);
      }
    }
    return additionalChanges;
  }
}
