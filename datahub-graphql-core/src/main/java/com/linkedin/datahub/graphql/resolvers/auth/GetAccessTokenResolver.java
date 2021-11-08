package com.linkedin.datahub.graphql.resolvers.auth;

import com.datahub.authentication.token.DataHubAccessTokenType;
import com.datahub.authentication.token.DataHubTokenService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AccessToken;
import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import com.linkedin.datahub.graphql.generated.AccessTokenType;
import com.linkedin.datahub.graphql.generated.GetAccessTokenInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Resolver for generating personal & service principal access tokens
 */
public class GetAccessTokenResolver implements DataFetcher<CompletableFuture<AccessToken>> {

  private final DataHubTokenService _tokenService;

  public GetAccessTokenResolver(final DataHubTokenService tokenService) {
    _tokenService = tokenService;
  }

  @Override
  public CompletableFuture<AccessToken> get(final DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      final QueryContext context = environment.getContext();
      final String authenticatedActor = context.getActor();
      final GetAccessTokenInput input = bindArgument(environment.getArgument("input"), GetAccessTokenInput.class);

      if (isAuthorizedToGenerateToken(authenticatedActor, input)) {
        final DataHubAccessTokenType type = DataHubAccessTokenType.valueOf(input.getType().toString()); // warn: if we are out of sync here there are problems.
        final String actorUrn = input.getActorUrn();
        final long expiresInMs = mapDurationToMs(input.getDuration());
        final String accessToken = _tokenService.generateAccessToken(type, actorUrn, expiresInMs);
        AccessToken result = new AccessToken();
        result.setAccessToken(accessToken);
        return result;
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private boolean isAuthorizedToGenerateToken(final String actor, GetAccessTokenInput input) {
    // TODO: Add a privilege for generating service access tokens.
    // Currently only an actor can generate a personal token for themselves.
    return input.getActorUrn().equals(actor) && AccessTokenType.PERSONAL.equals(input.getType());
  }

  private long mapDurationToMs(final AccessTokenDuration duration) {
    switch (duration) {
      case ONE_HOUR:
        return Duration.of(1, ChronoUnit.HOURS).toMillis();
      case ONE_DAY:
        return Duration.of(1, ChronoUnit.DAYS).toMillis();
      case ONE_MONTH:
        return Duration.of(30, ChronoUnit.DAYS).toMillis();
      case THREE_MONTHS:
        return Duration.of(90, ChronoUnit.DAYS).toMillis();
      case SIX_MONTHS:
        return Duration.of(180, ChronoUnit.DAYS).toMillis();
      case ONE_YEAR:
        return Duration.of(365, ChronoUnit.DAYS).toMillis();
      default:
        throw new RuntimeException(String.format("Unrecognized access token duration %s provided", duration));
    }
  }
}
