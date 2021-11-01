package com.datahub.authentication.authenticators;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationResult;
import com.datahub.authentication.Authenticator;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import java.util.Collections;
import java.util.Map;

import static com.datahub.authentication.Constants.*;


/**
 * Authenticator that verifies DataHub-issued JSON web tokens.
 */
public class DataHubTokenAuthenticator implements Authenticator {

  private static final String ACTOR_URN_CLAIM = "actorUrn";
  private static final String HS_256 = "HS256";

  private String signingKey;
  private String signingAlgorithm; // Supported are HS256

  @Override
  public void init(final Map<String, Object> config) {
    this.signingKey = (String) config.getOrDefault("signing_key", "YouKnowNothing");
    this.signingAlgorithm = (String) config.getOrDefault("signing_alg", "HS256");
    if (!HS_256.equals(this.signingAlgorithm)) {
      throw new UnsupportedOperationException(
          String.format("Failed to create token authenticator. Unsupported signing algorithm %s", this.signingAlgorithm));
    }
  }

  @Override
  public AuthenticationResult authenticate(AuthenticationContext context) {
    final String authorizationHeader = context.headers().get("Authorization"); // Case insensitive
    if (authorizationHeader != null) {
      if (authorizationHeader.startsWith("Bearer ") || authorizationHeader.startsWith("bearer ")) {
        return validateAndExtract(authorizationHeader.substring(7));
      } else {
        return FAILURE_AUTHENTICATION_RESULT; // TODO: Qualify this further.
      }
    }
    return FAILURE_AUTHENTICATION_RESULT;
  }

  private AuthenticationResult validateAndExtract(final String authorizationHeader) {
    final String token = authorizationHeader.substring(7);
    final Claims claims = (Claims) Jwts.parserBuilder()
        .setSigningKey(this.signingKey)
        .build()
        .parse(token)
        .getBody();
    final String actorUrn = claims.get(ACTOR_URN_CLAIM, String.class);
    if (actorUrn != null && actorUrn.length() > 0) {
      return new AuthenticationResult(
          AuthenticationResult.Type.SUCCESS,
          new Authentication(
              authorizationHeader,
              actorUrn,
              null,
              Collections.emptySet(),
              claims)
      );
    }
    return FAILURE_AUTHENTICATION_RESULT;
  }
}
