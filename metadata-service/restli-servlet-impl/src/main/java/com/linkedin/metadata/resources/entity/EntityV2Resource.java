package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_ASPECTS;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;


/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@Slf4j
@RestLiCollection(name = "entitiesV2", namespace = "com.linkedin.entity")
public class EntityV2Resource extends CollectionResourceTaskTemplate<String, EntityResponse> {

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  @WithSpan
  public Task<EntityResponse> get(@Nonnull String urnStr,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
    log.debug("GET V2 {}", urnStr);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      final String entityName = urnToEntityName(urn);
      final Set<String> projectedAspects =
          aspectNames == null ? getAllAspectNames(entityName) : new HashSet<>(Arrays.asList(aspectNames));
      try {
        return _entityService.getEntityV2(entityName, urn, projectedAspects);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to get entity with urn: %s, aspects: %s", urn, projectedAspects), e);
      }
    }, MetricRegistry.name(this.getClass(), "get"));
  }

  @RestMethod.BatchGet
  @Nonnull
  @WithSpan
  public Task<Map<Urn, EntityResponse>> batchGet(@Nonnull Set<String> urnStrs,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
    log.debug("BATCH GET V2 {}", urnStrs.toString());
    final Set<Urn> urns = new HashSet<>();
    for (final String urnStr : urnStrs) {
      urns.add(Urn.createFromString(urnStr));
    }
    if (urns.size() <= 0) {
      return Task.value(Collections.emptyMap());
    }
    final String entityName = urnToEntityName(urns.iterator().next());
    return RestliUtil.toTask(() -> {
      final Set<String> projectedAspects =
          aspectNames == null ? getAllAspectNames(entityName) : new HashSet<>(Arrays.asList(aspectNames));
      try {
        return _entityService.getEntitiesV2(entityName, urns, projectedAspects);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to batch get entities with urns: %s, projectedAspects: %s", urns, projectedAspects),
            e);
      }
    }, MetricRegistry.name(this.getClass(), "batchGet"));
  }

  private Set<String> getAllAspectNames(final String entityName) {
    return _entityService.getEntityAspectNames(entityName);
  }
}