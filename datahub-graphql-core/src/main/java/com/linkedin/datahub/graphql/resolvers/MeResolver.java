package com.linkedin.datahub.graphql.resolvers;

import com.linkedin.datahub.graphql.generated.AuthenticatedUser;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FeatureFlags;
import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;


/**
 * Generic GraphQL resolver responsible for
 *
 *    1. Retrieving a single input urn.
 *    2. Resolving a single {@link LoadableType}.
 *
 *  Note that this resolver expects that {@link DataLoader}s were registered
 *  for the provided {@link LoadableType} under the name provided by {@link LoadableType#name()}
 *
 */
public class MeResolver implements DataFetcher<CompletableFuture<AuthenticatedUser>> {

  @Override
  public CompletableFuture<AuthenticatedUser> get(DataFetchingEnvironment environment) {

    CorpUser user = new CorpUser();
    user.setType(EntityType.CORP_USER);
    user.setUsername("datahub");
    user.setUrn("urn:li:corpuser:datahub");

    FeatureFlags featureFlags = new FeatureFlags();
    featureFlags.setShowAnalytics(true);
    featureFlags.setShowAdminConsole(true);
    featureFlags.setShowPolicyBuilder(true);

    AuthenticatedUser authUser = new AuthenticatedUser();
    authUser.setCorpUser(user);
    authUser.setFeatures(featureFlags);

    return CompletableFuture.completedFuture(authUser);
  }
}
