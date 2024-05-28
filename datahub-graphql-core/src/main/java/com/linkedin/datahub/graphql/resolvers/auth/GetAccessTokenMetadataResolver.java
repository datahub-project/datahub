package com.linkedin.datahub.graphql.resolvers.auth;

import com.datahub.authentication.token.StatefulTokenService;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.types.auth.AccessTokenMetadataType;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetAccessTokenMetadataResolver
    implements DataFetcher<CompletableFuture<AccessTokenMetadata>> {

  private final StatefulTokenService _tokenService;
  private final EntityClient _entityClient;

  public GetAccessTokenMetadataResolver(
      final StatefulTokenService tokenService, EntityClient entityClient) {
    _tokenService = tokenService;
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<AccessTokenMetadata> get(final DataFetchingEnvironment environment)
      throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String token = environment.getArgument("token");
          log.info("User {} requesting access token metadata information.", context.getActorUrn());
          if (!AuthorizationUtils.canManageTokens(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          AccessTokenMetadataType metadataType = new AccessTokenMetadataType(_entityClient);
          final String tokenHash = _tokenService.hash(token);
          final String tokenUrn = _tokenService.tokenUrnFromKey(tokenHash).toString();
          try {
            List<DataFetcherResult<AccessTokenMetadata>> batchLoad =
                metadataType.batchLoad(ImmutableList.of(tokenUrn), context);
            if (batchLoad.isEmpty()) {
              return null;
            }
            return batchLoad.get(0).getData();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
