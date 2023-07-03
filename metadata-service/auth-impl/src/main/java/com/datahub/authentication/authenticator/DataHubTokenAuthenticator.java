package com.datahub.authentication.authenticator;

import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.token.StatelessTokenService;
import com.datahub.authentication.token.TokenClaims;
import com.datahub.authentication.token.TokenExpiredException;
import com.datahub.authentication.Actor;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationConstants;
import com.datahub.authentication.AuthenticationExpiredException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticationException;
import com.datahub.plugins.auth.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorContext;
import com.linkedin.metadata.entity.EntityService;
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
 *  - signingAlgorithm (optional): the algorithm used to verify JWT's. This should be THE SAME ONE used by the {@link StatelessTokenService}. Defaults to HS256.
 *  - signingKey: a key used to sign all JWT tokens using the provided signingAlgorithm
 */
@Slf4j
public class DataHubTokenAuthenticator implements Authenticator {

  public static final String SIGNING_KEY_CONFIG_NAME = "signingKey";
  public static final String SALT_CONFIG_NAME = "salt";
  public static final String SIGNING_ALG_CONFIG_NAME = "signingAlg";
  static final String DEFAULT_SIGNING_ALG = "HS256";
  static final String DEFAULT_ISSUER = "datahub-metadata-service";

  // Package-Visible for testing.
  StatefulTokenService _statefulTokenService;

  @Override
  public void init(@Nonnull final Map<String, Object> config, final AuthenticatorContext context) {
    Objects.requireNonNull(config, "Config parameter cannot be null");
    Objects.requireNonNull(context, "Context parameter cannot be null");
    final String signingKey =
        Objects.requireNonNull((String) config.get(SIGNING_KEY_CONFIG_NAME), "signingKey is a required config");
    final String salt =
        Objects.requireNonNull((String) config.get(SALT_CONFIG_NAME), "salt is a required config");
    final String signingAlgorithm = (String) config.getOrDefault(SIGNING_ALG_CONFIG_NAME, DEFAULT_SIGNING_ALG);
    log.debug(String.format("Creating TokenService using signing algorithm %s", signingAlgorithm));
    if (!context.data().containsKey(AuthenticationConstants.ENTITY_SERVICE)) {
      throw new IllegalArgumentException("Unable to initialize DataHubTokenAuthenticator, entity service reference not"
          + " found.");
    }
    final Object entityService = context.data().get(ENTITY_SERVICE);
    if (!(entityService instanceof EntityService)) {
      throw new RuntimeException(
          "Unable to initialize DataHubTokenAuthenticator, entity service reference is not of type: "
              + "EntityService.class, found: " + entityService.getClass());
    }
    this._statefulTokenService = (StatefulTokenService) Objects.requireNonNull(context.data().get(TOKEN_SERVICE));
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest context) throws AuthenticationException {
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
      final TokenClaims claims = this._statefulTokenService.validateAccessToken(token);
      return new Authentication(new Actor(claims.getActorType(), claims.getActorId()), credentials, claims.asMap());
    } catch (TokenExpiredException e) {
      throw new AuthenticationExpiredException(e.getMessage(), e);
    } catch (Exception e) {
      // Failed to validate the token
      throw new AuthenticationException("Unable to verify the provided token.", e);
    }
  }
}
