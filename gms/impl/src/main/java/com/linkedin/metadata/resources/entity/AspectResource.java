package com.linkedin.metadata.resources.entity;

import com.linkedin.aspect.GetAspectResponse;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.net.URISyntaxException;
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

  private static final String ACTION_GET_ASPECT = "getAspectValues";
  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ASPECT = "aspect";
  @Inject
  @Named("entityService")
  private EntityService _entityService;

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

  @Action(name = ACTION_GET_ASPECT)
  @Nonnull
  public Task<GetAspectResponse> getAspect(@ActionParam(PARAM_URN) @Nonnull String entityName,
      @ActionParam(PARAM_ASPECT) @Nonnull String aspectName,
      @ActionParam(FINDER_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_LIMIT) @Optional long limit) {
    log.info("Get Aspect values for aspect {} for entity {} with filter {} and limit {}.", aspectName, entityName,
        filter, limit);
    return RestliUtils.toTask(() -> {
      GetAspectResponse response = new GetAspectResponse();
      response.setEntityName(entityName);
      response.setAspectName(aspectName);
      response.setFilter(filter);
      response.setLimit(limit);
      response.setValues(new EnvelopedAspectArray(_entityService.getAspect(entityName, aspectName, filter, limit)));
      return response;
    });
  }
}
