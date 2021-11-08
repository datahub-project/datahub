package com.datahub.authentication.authenticators;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationResult;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.token.DataHubAccessTokenClaims;
import com.datahub.authentication.token.DataHubTokenService;
import java.util.Collections;
import java.util.Map;

import static com.datahub.authentication.Constants.*;


/**
 * Authenticator that verifies DataHub-issued JSON web tokens.
 */
public class DataHubTokenAuthenticator implements Authenticator {

  private DataHubTokenService tokenService;

  @Override
  public void init(final Map<String, Object> config) {
    final String signingKey = (String) config.getOrDefault("signing_key", "YouKnowNothing");
    final String signingAlgorithm = (String) config.getOrDefault("signing_alg", "HS256");
    this.tokenService = new DataHubTokenService(signingKey, signingAlgorithm, "datahubapp");
  }

  @Override
  public AuthenticationResult authenticate(AuthenticationContext context) {
    final String authorizationHeader = context.headers().get("Authorization"); // Case insensitive
    if (authorizationHeader != null) {
      if (authorizationHeader.startsWith("Bearer ") || authorizationHeader.startsWith("bearer ")) {
        final String token = authorizationHeader.substring(7);
        return validateAndExtract(token);
      } else {
        return FAILURE_AUTHENTICATION_RESULT; // TODO: Qualify this further.
      }
    }
    return FAILURE_AUTHENTICATION_RESULT;
  }

  private AuthenticationResult validateAndExtract(final String token) {
    try {
      final DataHubAccessTokenClaims claims = this.tokenService.validateAccessToken(token);
      return new AuthenticationResult(
          AuthenticationResult.Type.SUCCESS,
          new Authentication(
              token,
              claims.getActorUrn(),
              null,
              Collections.emptySet(),
              Collections.emptyMap())
      );
    } catch (Exception e) {
      // Failed to validate the token
      return FAILURE_AUTHENTICATION_RESULT;
    }
  }
}
