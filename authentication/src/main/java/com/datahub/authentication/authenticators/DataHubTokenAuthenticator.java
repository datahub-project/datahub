package com.datahub.authentication.authenticators;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.token.TokenClaims;
import com.datahub.authentication.token.TokenService;
import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static com.datahub.authentication.Constants.*;


/**
 * Authenticator that verifies DataHub-issued JSON web tokens.
 */
public class DataHubTokenAuthenticator implements Authenticator {

  private static final String SIGNING_KEY_CONFIG_NAME = "signingKey";
  private static final String SIGNING_ALG_CONFIG_NAME = "signingAlg";
  private static final String DEFAULT_SIGNING_ALG = "HS256";
  private static final String DEFAULT_ISS = "datahub-metadata-service";

  private TokenService tokenService;

  @Override
  public void init(@Nonnull final Map<String, Object> config) {
    Objects.requireNonNull(config);
    final String signingKey = (String) config.get(SIGNING_KEY_CONFIG_NAME);
    final String signingAlgorithm = (String) config.getOrDefault(SIGNING_ALG_CONFIG_NAME, DEFAULT_SIGNING_ALG);
    this.tokenService = new TokenService(signingKey, signingAlgorithm, DEFAULT_ISS);
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationContext context) throws AuthenticationException {
    Objects.requireNonNull(context);
    final String authorizationHeader = context.getRequestHeaders().get(AUTHORIZATION_HEADER_NAME); // Case insensitive
    if (authorizationHeader != null) {
      if (authorizationHeader.startsWith("Bearer ") || authorizationHeader.startsWith("bearer ")) {
        final String token = authorizationHeader.substring(7);
        return validateAndExtract(token);
      } else {
        throw new AuthenticationException("Authorization header missing 'Bearer' prefix.");
      }
    }
    throw new AuthenticationException("Request is missing 'Authorization' header.");
  }

  private Authentication validateAndExtract(final String token) throws AuthenticationException {
    try {
      final TokenClaims claims = this.tokenService.validateAccessToken(token);
      return new Authentication(
          new Actor(claims.getActorType(), claims.getActorId()),
          token,
          Collections.emptySet(),
          Collections.emptyMap(),
          null);
    } catch (Exception e) {
      // Failed to validate the token
      throw new AuthenticationException("Unable to verify the provided JWT.", e);
    }
  }
}
