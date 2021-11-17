package com.linkedin.datahub.graphql.resolvers.auth;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.token.TokenType;
import com.datahub.authentication.token.TokenService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AccessToken;
import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import com.linkedin.datahub.graphql.generated.AccessTokenType;
import com.linkedin.datahub.graphql.generated.GetAccessTokenInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Resolver for generating personal & service principal access tokens
 */
@Slf4j
public class GetAccessTokenResolver implements DataFetcher<CompletableFuture<AccessToken>> {

  private final TokenService _tokenService;

  public GetAccessTokenResolver(final TokenService tokenService) {
    _tokenService = tokenService;
  }

  @Override
  public CompletableFuture<AccessToken> get(final DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      final QueryContext context = environment.getContext();
      final GetAccessTokenInput input = bindArgument(environment.getArgument("input"), GetAccessTokenInput.class);

      if (isAuthorizedToGenerateToken(context, input)) {
        final TokenType type = TokenType.valueOf(input.getType().toString()); // warn: if we are out of sync here there are problems.
        final String actorUrn = input.getActorUrn();
        final long expiresInMs = mapDurationToMs(input.getDuration());
        final String accessToken = _tokenService.generateAccessToken(type, createActor(input.getType(), actorUrn), expiresInMs);
        AccessToken result = new AccessToken();
        result.setAccessToken(accessToken);
        return result;
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private boolean isAuthorizedToGenerateToken(final QueryContext context, final GetAccessTokenInput input) {
    // Currently only an actor can generate a personal token for themselves.
    if (AccessTokenType.PERSONAL.equals(input.getType())) {
      return isAuthorizedToGeneratePersonalAccessToken(context, input);
    }
    throw new UnsupportedOperationException(String.format("Unsupported AccessTokenType %s provided", input.getType()));
  }

  private boolean isAuthorizedToGeneratePersonalAccessToken(final QueryContext context, final GetAccessTokenInput input) {
    return input.getActorUrn().equals(context.getActor()) && AuthorizationUtils.canGeneratePersonalAccessToken(context);
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

  private Actor createActor(AccessTokenType tokenType, String actorUrn) {
    if (AccessTokenType.PERSONAL.equals(tokenType)) {
      // If we are generating a personal access token, then the actor will be of "USER" type.
      return new Actor(
          ActorType.USER,
          createUrn(actorUrn).getId()
      );
    }
    throw new IllegalArgumentException(String.format("Unsupported token type %s provided", tokenType));
  }

  private Urn createUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Failed to validate provided urn %s", urnStr));
    }
  }
}
