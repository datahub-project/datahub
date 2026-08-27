package com.linkedin.metadata.resources.entity;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.datahub.authorization.AuthUtil.isAPIAuthorizedUrns;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.authorization.EntityAuthorizationUtils.isAPIAuthorizedEntityUrns;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
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
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
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

/**
 * Single unified resource for fetching, updating, searching, & browsing versioned DataHub entities
 */
@Slf4j
@RestLiCollection(
    name = "entitiesVersionedV2",
    namespace = "com.linkedin.entity",
    keyTyperefClass = com.linkedin.common.versioned.VersionedUrn.class)
public class EntityVersionedV2Resource
    extends CollectionResourceTaskTemplate<com.linkedin.common.urn.VersionedUrn, EntityResponse> {

  @Inject
  @Named("entityService")
  private EntityService<?> _entityService;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

    @Inject
    @Named("systemOperationContext")
    private OperationContext systemOperationContext;

  @VisibleForTesting
  void setEntityService(EntityService<?> entityService) {
    this._entityService = entityService;
  }

  @VisibleForTesting
  void setAuthorizer(Authorizer authorizer) {
    this._authorizer = authorizer;
  }

  @VisibleForTesting
  void setSystemOperationContext(OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
  }

  @RestMethod.BatchGet
  @Nonnull
  @WithSpan
  public Task<Map<Urn, EntityResponse>> batchGetVersioned(
      @Nonnull Set<com.linkedin.common.urn.VersionedUrn> versionedUrnStrs,
      @QueryParam(PARAM_ENTITY_TYPE) @Nonnull String entityType,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

      Set<Urn> urns = versionedUrnStrs.stream()
              .map(versionedUrn -> UrnUtils.getUrn(versionedUrn.getUrn())).collect(Collectors.toSet());

      Authentication auth = AuthenticationContext.getAuthentication();
      final OperationContext opContext = RestliUtils.asSession(
              systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(), "authorizerChain", urns.stream()
                      .map(Urn::getEntityType).collect(Collectors.toList())).withUsageOperation(UsageOperation.METADATA_READ), _authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            READ,
            urns)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN,
          "User is unauthorized to get entities " + versionedUrnStrs);
    }


      log.debug("BATCH GET VERSIONED V2 {}", versionedUrnStrs);
    if (versionedUrnStrs.size() <= 0) {
      return Task.value(Collections.emptyMap());
    }
    return RestliUtils.toTask(opContext,
        () -> {
          final Set<String> projectedAspects =
              aspectNames == null
                  ? opContext.getEntityAspectNames(entityType)
                  : new HashSet<>(Arrays.asList(aspectNames));
          try {
            Map<Urn, EntityResponse> response =
                _entityService.getEntitiesVersionedV2(opContext,
                    versionedUrnStrs.stream()
                        .map(
                            versionedUrnTyperef -> {
                              VersionedUrn versionedUrn =
                                  new VersionedUrn()
                                      .setUrn(UrnUtils.getUrn(versionedUrnTyperef.getUrn()));
                              if (versionedUrnTyperef.getVersionStamp() != null) {
                                versionedUrn.setVersionStamp(versionedUrnTyperef.getVersionStamp());
                              }
                              return versionedUrn;
                            })
                        .collect(Collectors.toSet()),
                    projectedAspects);
            // This endpoint has no per-field mapper the way GraphQL has, so SQL-bearing aspects
            // the actor lacks VIEW_ENTITY_QUERIES for are redacted from the response here, same
            // as EntityV2Resource/EntitiesController do for the non-versioned equivalents.
            EntityAuthorizationUtils.completelyRedactUnauthorizedQuerySqlAspects(
                opContext, response);
            return response;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to batch get versioned entities: %s, projectedAspects: %s",
                    versionedUrnStrs, projectedAspects),
                e);
          }
        },
        MetricRegistry.name(this.getClass(), "batchGet"));
  }
}
