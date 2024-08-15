package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.token.StatefulTokenService;
import com.google.common.collect.ImmutableSet;
import com.linkedin.access.token.DataHubAccessTokenInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver for revoking personal & service principal v2-type (stateful) access tokens. */
@Slf4j
public class RevokeAccessTokenResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final StatefulTokenService _statefulTokenService;

  public RevokeAccessTokenResolver(
      final EntityClient entityClient, final StatefulTokenService statefulTokenService) {
    _entityClient = entityClient;
    _statefulTokenService = statefulTokenService;
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String tokenId = bindArgument(environment.getArgument("tokenId"), String.class);

          log.info("User {} revoking access token", context.getActorUrn());

          if (isAuthorizedToRevokeToken(context, tokenId)) {
            try {
              _statefulTokenService.revokeAccessToken(tokenId);
            } catch (Exception e) {
              throw new RuntimeException("Failed to revoke access token", e);
            }
            return true;
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private boolean isAuthorizedToRevokeToken(final QueryContext context, final String tokenId) {
    return AuthorizationUtils.canManageTokens(context) || isOwnerOfAccessToken(context, tokenId);
  }

  private boolean isOwnerOfAccessToken(final QueryContext context, final String tokenId) {
    try {
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              Constants.ACCESS_TOKEN_ENTITY_NAME,
              Urn.createFromTuple(Constants.ACCESS_TOKEN_ENTITY_NAME, tokenId),
              ImmutableSet.of(Constants.ACCESS_TOKEN_INFO_NAME));

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(Constants.ACCESS_TOKEN_INFO_NAME)) {
        final DataMap data =
            entityResponse.getAspects().get(Constants.ACCESS_TOKEN_INFO_NAME).getValue().data();
        final DataHubAccessTokenInfo tokenInfo = new DataHubAccessTokenInfo(data);
        return tokenInfo.getOwnerUrn().toString().equals(context.getActorUrn());
      }

      return false;
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
