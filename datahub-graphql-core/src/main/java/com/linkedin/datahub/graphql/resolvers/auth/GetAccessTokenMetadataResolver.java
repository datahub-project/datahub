package com.linkedin.datahub.graphql.resolvers.auth;

import com.datahub.authentication.token.StatefulTokenService;
import com.linkedin.access.token.DataHubAccessTokenInfo;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.generated.EntityType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetAccessTokenMetadataResolver
    implements DataFetcher<CompletableFuture<AccessTokenMetadata>> {

  private final StatefulTokenService _tokenService;

  public GetAccessTokenMetadataResolver(final StatefulTokenService tokenService) {
    _tokenService = tokenService;
  }

  @Override
  public CompletableFuture<AccessTokenMetadata> get(final DataFetchingEnvironment environment)
      throws Exception {
    return CompletableFuture.supplyAsync(
        () -> {
          final String token = environment.getArgument("token");
          if (!AuthorizationUtils.canManageTokens(environment.getContext())) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          final DataHubAccessTokenInfo tokenInfo = _tokenService.getAccessTokenInfo(token);
          if (tokenInfo == null) {
            return null;
          }
          AccessTokenMetadata metadata = new AccessTokenMetadata();
          metadata.setId(_tokenService.hash(token));
          metadata.setType(EntityType.ACCESS_TOKEN);
          metadata.setName(tokenInfo.getName());
          metadata.setDescription(tokenInfo.getDescription());
          metadata.setActorUrn(tokenInfo.getActorUrn().toString());
          metadata.setOwnerUrn(tokenInfo.getOwnerUrn().toString());
          metadata.setCreatedAt(tokenInfo.getCreatedAt());
          metadata.setExpiresAt(tokenInfo.getExpiresAt());
          return metadata;
        });
  }
}
