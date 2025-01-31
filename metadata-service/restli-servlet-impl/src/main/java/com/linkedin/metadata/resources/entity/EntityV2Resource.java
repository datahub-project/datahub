package com.linkedin.metadata.resources.entity;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.datahub.authorization.AuthUtil.isAPIAuthorizedEntityUrns;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/** Single unified resource for fetching, updating, searching, & browsing DataHub entities */
@Slf4j
@RestLiCollection(name = "entitiesV2", namespace = "com.linkedin.entity")
public class EntityV2Resource extends CollectionResourceTaskTemplate<String, EntityResponse> {

  @Inject
  @Named("entityService")
  private EntityService<?> _entityService;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

    @Inject
    @Named("systemOperationContext")
    private OperationContext systemOperationContext;

  /** Retrieves the value for an entity that is made up of latest versions of specified aspects. */
  @RestMethod.Get
  @Nonnull
  @WithSpan
  public Task<EntityResponse> get(
      @Nonnull String urnStr, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_ALWAYS_INCLUDE_KEY_ASPECT) @Optional @Nullable Boolean alwaysIncludeKeyAspect)
      throws URISyntaxException {
    log.debug("GET V2 {}", urnStr);
    final Urn urn = Urn.createFromString(urnStr);

      final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "getEntityV2", urn.getEntityType()), _authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            READ,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity " + urn);
    }

      return RestliUtils.toTask(systemOperationContext,
        () -> {
          final String entityName = urnToEntityName(urn);
          final Set<String> projectedAspects =
              aspectNames == null
                  ? opContext.getEntityAspectNames(entityName)
                  : new HashSet<>(Arrays.asList(aspectNames));
          try {
            return _entityService.getEntityV2(opContext, entityName, urn, projectedAspects, alwaysIncludeKeyAspect == null || alwaysIncludeKeyAspect);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to get entity with urn: %s, aspects: %s", urn, projectedAspects),
                e);
          }
        },
        MetricRegistry.name(this.getClass(), "get"));
  }

  @RestMethod.BatchGet
  @Nonnull
  @WithSpan
  public Task<Map<Urn, EntityResponse>> batchGet(
      @Nonnull Set<String> urnStrs,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_ALWAYS_INCLUDE_KEY_ASPECT) @Optional @Nullable Boolean alwaysIncludeKeyAspect)
      throws URISyntaxException {
    log.debug("BATCH GET V2 {}", urnStrs.toString());
    final Set<Urn> urns = new HashSet<>();
    for (final String urnStr : urnStrs) {
      urns.add(Urn.createFromString(urnStr));
    }

      final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "getEntityV2", urns.stream().map(Urn::getEntityType).collect(Collectors.toList())), _authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            READ,
            urns)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entities " + urnStrs);
    }

      if (urns.size() <= 0) {
      return Task.value(Collections.emptyMap());
    }
    final String entityName = urnToEntityName(urns.iterator().next());
    return RestliUtils.toTask(systemOperationContext,
        () -> {
          final Set<String> projectedAspects =
              aspectNames == null
                  ? opContext.getEntityAspectNames(entityName)
                  : new HashSet<>(Arrays.asList(aspectNames));
          try {
            return _entityService.getEntitiesV2(opContext, entityName, urns, projectedAspects, alwaysIncludeKeyAspect == null || alwaysIncludeKeyAspect);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to batch get entities with urns: %s, projectedAspects: %s",
                    urns, projectedAspects),
                e);
          }
        },
        MetricRegistry.name(this.getClass(), "batchGet"));
  }
}
