package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.token.TokenException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.authentication.AuthenticationConstants.*;


/**
 * Authenticator that verifies legacy API-gateway compliant JSON web tokens for Acryl SaaS (generated inside Acryl Admin Server).
 *
 * This authenticator requires the following configurations:
 *
 *  - signingKey: a key used to sign all JWT tokens using the provided signingAlgorithm.
 *    This should be THE SAME ONE used by Acryl Admin Server to mint new auth tokens.
 */
@Slf4j
public class LegacyTokenAuthenticator implements Authenticator {

  private static final String ACRYL_ISSUER = "admin.acryl.io"; // The issuer used to generate the Acryl Admin Server Token
  static final String SIGNING_KEY_CONFIG_NAME = "signingKey";
  static final String DEFAULT_SIGNING_ALG = "HS256";
  static final String DEFAULT_ISSUER = "datahub-metadata-service";

  private String signingKey;

  @Override
  public void init(@Nonnull final Map<String, Object> config, @Nullable final AuthenticatorContext context) {
    Objects.requireNonNull(config, "Config parameter cannot be null");
    final String signingKey = Objects.requireNonNull((String) config.get(SIGNING_KEY_CONFIG_NAME), "signingKey is a required config");
    log.debug("Creating LegacyTokenService");
    this.signingKey = signingKey;
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
    log.debug("Found potential legacy authentication token. Verifying...");
    final String token = credentials.substring(7);
    try {
      if (isValidToken(token)) {
        return new Authentication(
            new Actor(ActorType.USER, "admin"), // Fallback to using the Admin actor.
            credentials, Collections.emptyMap()
        );
      } else {
        throw new AuthenticationException("Unable to verify the provided token. Token is invalid.");
      }
    } catch (Exception e) {
      // Failed to validate the token
      throw new AuthenticationException("Unable to verify the provided token.", e);
    }
  }

  private boolean isValidToken(final String token) throws TokenException {
    Objects.requireNonNull(token);
    Objects.requireNonNull(this.signingKey);
    try {
      byte [] apiKeySecretBytes = this.signingKey.getBytes(StandardCharsets.UTF_8);
      final String base64Key = Base64.getEncoder().encodeToString(apiKeySecretBytes);
      final Claims claims = (Claims) Jwts.parserBuilder()
          .setSigningKey(base64Key)
          .build()
          .parse(token)
          .getBody();
      final String issuer = claims.get("iss", String.class);
      if (!issuer.equals(ACRYL_ISSUER)) {
        throw new TokenException(String.format("Failed to validate DataHub Token. Not issued by %s", ACRYL_ISSUER));
      }
      return true;
    } catch (Exception e) {
      throw new TokenException("Failed to validate DataHub token", e);
    }
  }
}
