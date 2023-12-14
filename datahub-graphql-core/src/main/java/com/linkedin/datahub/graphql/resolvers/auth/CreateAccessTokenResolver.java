package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.token.TokenType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AccessToken;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.generated.AccessTokenType;
import com.linkedin.datahub.graphql.generated.CreateAccessTokenInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver for creating personal & service principal v2-type (stateful) access tokens. */
@Slf4j
public class CreateAccessTokenResolver implements DataFetcher<CompletableFuture<AccessToken>> {

  private final StatefulTokenService _statefulTokenService;

  public CreateAccessTokenResolver(final StatefulTokenService statefulTokenService) {
    _statefulTokenService = statefulTokenService;
  }

  @Override
  public CompletableFuture<AccessToken> get(final DataFetchingEnvironment environment)
      throws Exception {
    return CompletableFuture.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final CreateAccessTokenInput input =
              bindArgument(environment.getArgument("input"), CreateAccessTokenInput.class);

          log.info(
              "User {} requesting new access token for user {} ",
              context.getActorUrn(),
              input.getActorUrn());

          if (isAuthorizedToGenerateToken(context, input)) {
            final TokenType type =
                TokenType.valueOf(
                    input
                        .getType()
                        .toString()); // warn: if we are out of sync with AccessTokenType there are
            // problems.
            final String actorUrn = input.getActorUrn();
            final Date date = new Date();
            final long createdAtInMs = date.getTime();
            final Optional<Long> expiresInMs = AccessTokenUtil.mapDurationToMs(input.getDuration());

            final String tokenName = input.getName();
            final String tokenDescription = input.getDescription();

            final String accessToken =
                _statefulTokenService.generateAccessToken(
                    type,
                    createActor(input.getType(), actorUrn),
                    expiresInMs.orElse(null),
                    createdAtInMs,
                    tokenName,
                    tokenDescription,
                    context.getActorUrn());
            log.info(
                "Generated access token for {} of type {} with duration {}",
                input.getActorUrn(),
                input.getType(),
                input.getDuration());
            try {
              final String tokenHash = _statefulTokenService.hash(accessToken);

              final AccessToken result = new AccessToken();
              result.setAccessToken(accessToken);
              final AccessTokenMetadata metadata = new AccessTokenMetadata();
              metadata.setUrn(
                  Urn.createFromTuple(Constants.ACCESS_TOKEN_ENTITY_NAME, tokenHash).toString());
              metadata.setType(EntityType.ACCESS_TOKEN);
              result.setMetadata(metadata);

              return result;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to create new access token with name %s", input.getName()),
                  e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private boolean isAuthorizedToGenerateToken(
      final QueryContext context, final CreateAccessTokenInput input) {
    if (AccessTokenType.PERSONAL.equals(input.getType())) {
      return isAuthorizedToGeneratePersonalAccessToken(context, input);
    }
    throw new UnsupportedOperationException(
        String.format("Unsupported AccessTokenType %s provided", input.getType()));
  }

  private boolean isAuthorizedToGeneratePersonalAccessToken(
      final QueryContext context, final CreateAccessTokenInput input) {
    return AuthorizationUtils.canManageTokens(context)
        || input.getActorUrn().equals(context.getActorUrn())
            && AuthorizationUtils.canGeneratePersonalAccessToken(context);
  }

  private Actor createActor(AccessTokenType tokenType, String actorUrn) {
    if (AccessTokenType.PERSONAL.equals(tokenType)) {
      // If we are generating a personal access token, then the actor will be of "USER" type.
      return new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId());
    }
    throw new IllegalArgumentException(
        String.format("Unsupported token type %s provided", tokenType));
  }
}
