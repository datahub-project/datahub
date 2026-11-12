package com.linkedin.metadata.usage.instrumentation;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.token.TokenClaimNames;
import com.datahub.authentication.token.TokenType;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.usage.AuthChannel;
import java.util.Map;
import javax.annotation.Nonnull;

public final class UsageAuthChannelResolver {

  private UsageAuthChannelResolver() {}

  @Nonnull
  public static AuthChannel resolve(
      @Nonnull Authentication authentication, @Nonnull String actorUrn) {
    if (Constants.SYSTEM_ACTOR.equals(actorUrn) || Constants.ANONYMOUS_ACTOR.equals(actorUrn)) {
      return Constants.SYSTEM_ACTOR.equals(actorUrn) ? AuthChannel.SYSTEM : AuthChannel.ANONYMOUS;
    }
    String credentials = authentication.getCredentials();
    if (credentials == null || credentials.isBlank()) {
      return AuthChannel.UNKNOWN;
    }
    String upper = credentials.trim().toUpperCase();
    if (upper.startsWith("BASIC ")) {
      return AuthChannel.SESSION;
    }
    if (upper.startsWith("BEARER ")) {
      return resolveBearerChannel(authentication);
    }
    return AuthChannel.UNKNOWN;
  }

  @Nonnull
  private static AuthChannel resolveBearerChannel(@Nonnull Authentication authentication) {
    Map<String, Object> claims = authentication.getClaims();
    if (claims == null || claims.isEmpty()) {
      // External OAuth / third-party JWT authenticators do not attach DataHub token-type claims.
      return AuthChannel.OAUTH;
    }
    Object tokenType = claims.get(TokenClaimNames.TOKEN_TYPE);
    if (tokenType == null) {
      return AuthChannel.OAUTH;
    }
    try {
      return switch (TokenType.valueOf(tokenType.toString())) {
        case PERSONAL, SERVICE_ACCOUNT -> AuthChannel.PAT;
        case SESSION -> AuthChannel.SESSION;
      };
    } catch (IllegalArgumentException e) {
      // External OAuth / third-party JWT token types not in TokenType enum.
      return AuthChannel.OAUTH;
    }
  }
}
