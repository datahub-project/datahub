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
import com.linkedin.datahub.graphql.generated.LinkVersionInput;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.entity.versioning.VersionPropertiesInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang.StringUtils;

/**
 * Currently only supports linking the latest version, but may be modified later to support inserts
 */
public class LinkAssetVersionResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityVersioningService entityVersioningService;
  private final FeatureFlags featureFlags;

  public LinkAssetVersionResolver(
      EntityVersioningService entityVersioningService, FeatureFlags featureFlags) {
    this.entityVersioningService = entityVersioningService;
    this.featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final LinkVersionInput input =
        bindArgument(environment.getArgument("input"), LinkVersionInput.class);
    if (!featureFlags.isEntityVersioning()) {
      throw new IllegalAccessError(
          "Entity Versioning is not configured, please enable before attempting to use this feature.");
    }
    Urn versionSetUrn = UrnUtils.getUrn(input.getVersionSet());
    if (!VERSION_SET_ENTITY_NAME.equals(versionSetUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Version Set urn %s must be of type Version Set.", input.getVersionSet()));
    }
    Urn entityUrn = UrnUtils.getUrn(input.getLinkedEntity());
    OperationContext opContext = context.getOperationContext();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        opContext, UPDATE, ImmutableSet.of(versionSetUrn, entityUrn))) {
      throw new AuthorizationException(
          String.format(
              "%s is unauthorized to %s entities %s and %s",
              opContext.getAuthentication().getActor().toUrnStr(),
              UPDATE,
              input.getVersionSet(),
              input.getLinkedEntity()));
    }
    VersionPropertiesInput versionPropertiesInput =
        new VersionPropertiesInput(
            input.getComment(),
            input.getVersion(),
            input.getSourceTimestamp(),
            input.getSourceCreator());
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          List<IngestResult> linkResults =
              entityVersioningService.linkLatestVersion(
                  opContext, versionSetUrn, entityUrn, versionPropertiesInput);

          return linkResults.stream()
              .filter(
                  ingestResult -> input.getLinkedEntity().equals(ingestResult.getUrn().toString()))
              .map(ingestResult -> ingestResult.getUrn().toString())
              .findAny()
              .orElse(StringUtils.EMPTY);
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
