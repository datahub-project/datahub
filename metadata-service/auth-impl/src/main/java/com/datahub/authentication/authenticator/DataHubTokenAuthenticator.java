package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.token.TokenClaims;
import com.datahub.authentication.token.TokenService;
import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.authentication.AuthenticationConstants.*;


/**
 * Authenticator that verifies DataHub-issued JSON web tokens.
 *
 * This authenticator requires the following configurations:
 *
 *  - signingAlgorithm (optional): the algorithm used to verify JWT's. This should be THE SAME ONE used by the {@link TokenService}. Defaults to HS256.
 *  - signingKey: a key used to sign all JWT tokens using the provided signingAlgorithm
 */
@Slf4j
public class DataHubTokenAuthenticator implements Authenticator {

  static final String SIGNING_KEY_CONFIG_NAME = "signingKey";
  static final String SIGNING_ALG_CONFIG_NAME = "signingAlg";
  static final String DEFAULT_SIGNING_ALG = "HS256";
  static final String DEFAULT_ISSUER = "datahub-metadata-service";

  // Package-Visible for testing.
  TokenService tokenService;

  @Override
  public void init(@Nonnull final Map<String, Object> config) {
    Objects.requireNonNull(config, "Config parameter cannot be null");
    final String signingKey = Objects.requireNonNull((String) config.get(SIGNING_KEY_CONFIG_NAME), "signingKey is a required config");
    final String signingAlgorithm = (String) config.getOrDefault(SIGNING_ALG_CONFIG_NAME, DEFAULT_SIGNING_ALG);
    log.debug(String.format("Creating TokenService using signing algorithm %s", signingAlgorithm));
    this.tokenService = new TokenService(signingKey, signingAlgorithm, DEFAULT_ISSUER);
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticatorContext context) throws AuthenticationException {
    Objects.requireNonNull(context);
    final String authorizationHeader = context.getRequestHeaders().get(AUTHORIZATION_HEADER_NAME); // Case insensitive
    if (authorizationHeader != null) {
      if (authorizationHeader.startsWith("Bearer ") || authorizationHeader.startsWith("bearer ")) {
        return validateAndExtract(authorizationHeader);
      } else {
        throw new AuthenticationException("Authorization header missing 'Bearer' prefix.");
      }
    }
    throw new AuthenticationException("Request is missing 'Authorization' header.");
  }

  private Authentication validateAndExtract(final String credentials) throws AuthenticationException {
    log.debug("Found authentication token. Verifying...");
    final String token = credentials.substring(7);
    try {
      final TokenClaims claims = this.tokenService.validateAccessToken(token);
      return new Authentication(
          new Actor(claims.getActorType(), claims.getActorId()),
          credentials,
          claims.asMap());
    } catch (Exception e) {
      // Failed to validate the token
      throw new AuthenticationException("Unable to verify the provided token.", e);
    }
  }
}
