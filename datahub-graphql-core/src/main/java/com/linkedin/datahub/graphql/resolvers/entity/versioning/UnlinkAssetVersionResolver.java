package com.linkedin.datahub.graphql.resolvers.entity.versioning;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.VERSION_SET_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authorization.AuthUtil;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.UnlinkVersionInput;
import com.linkedin.datahub.graphql.generated.VersionSet;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class UnlinkAssetVersionResolver implements DataFetcher<CompletableFuture<VersionSet>> {

  private final EntityVersioningService entityVersioningService;
  private final FeatureFlags featureFlags;

  public UnlinkAssetVersionResolver(
      EntityVersioningService entityVersioningService, FeatureFlags featureFlags) {
    this.entityVersioningService = entityVersioningService;
    this.featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<VersionSet> get(DataFetchingEnvironment environment) throws Exception {
    if (!featureFlags.isEntityVersioning()) {
      throw new IllegalAccessError(
          "Entity Versioning is not configured, please enable before attempting to use this feature.");
    }
    final QueryContext context = environment.getContext();
    final UnlinkVersionInput input =
        bindArgument(environment.getArgument("input"), UnlinkVersionInput.class);
    Urn versionSetUrn = UrnUtils.getUrn(input.getVersionSet());
    if (!VERSION_SET_ENTITY_NAME.equals(versionSetUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Version Set urn %s must be of type Version Set.", input.getVersionSet()));
    }
    Urn entityUrn = UrnUtils.getUrn(input.getUnlinkedEntity());
    OperationContext opContext = context.getOperationContext();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        opContext, UPDATE, ImmutableSet.of(versionSetUrn, entityUrn))) {
      throw new AuthorizationException(
          String.format(
              "%s is unauthorized to %s entities %s and %s",
              opContext.getAuthentication().getActor(),
              UPDATE,
              input.getVersionSet(),
              input.getUnlinkedEntity()));
    }
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          List<RollbackResult> results =
              entityVersioningService.unlinkVersion(opContext, versionSetUrn, entityUrn);
          if (results.isEmpty() || results.stream().allMatch(RollbackResult::isNoOp)) {
            return null;
          }
          VersionSet versionSet = new VersionSet();
          versionSet.setUrn(versionSetUrn.toString());
          versionSet.setType(EntityType.VERSION_SET);
          return versionSet;
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
