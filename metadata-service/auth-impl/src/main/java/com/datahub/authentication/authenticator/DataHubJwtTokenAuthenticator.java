package com.datahub.authentication.authenticator;

import static com.datahub.authentication.AuthenticationConstants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.token.DataHubJwtSigningKeyResolver;
import com.datahub.plugins.auth.authentication.Authenticator;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This Authenticator verifies third party token and allows to pass claim for "id" part of resolved
 * actor urn. Supported algorithm at this moment RSA
 */
@Slf4j
public class DataHubJwtTokenAuthenticator implements Authenticator {

  static final String DEFAULT_USER_CLAIM = "aud";

  static final String DEFAULT_SIGNING_ALG = "RSA";

  /**
   * idUserClaim allows you to select which claim will be used as the "id" part of the resolved
   * actor urn, e.g. "urn:li:corpuser:" *
   */
  private String userIdClaim;

  /** List of trusted issuers * */
  private HashSet<String> trustedIssuers;

  /**
   * This public key is optional and should be used if token public key is not available online or
   * will not change for signed token. *
   */
  private String publicKey;

  /**
   * Algorithm used to sign your token. This is optional and can be skiped if public key is
   * available online. *
   */
  private String algorithm;

  @Override
  public void init(
      @Nonnull final Map<String, Object> config, @Nullable final AuthenticatorContext context) {
    Objects.requireNonNull(config, "Config parameter cannot be null");

    this.userIdClaim =
        config.get("userIdClaim") == null ? DEFAULT_USER_CLAIM : (String) config.get("userIdClaim");

    Map<String, String> issuers =
        Objects.requireNonNull(
            (Map<String, String>) config.get("trustedIssuers"),
            "Missing required config trusted issuers");
    this.trustedIssuers = new HashSet<String>(issuers.values());

    this.publicKey = (String) config.get("publicKey");
    this.algorithm =
        config.get("algorithm") == null ? DEFAULT_SIGNING_ALG : (String) config.get("algorithm");
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest context)
      throws AuthenticationException {
    Objects.requireNonNull(context);

    try {
      String jwtToken = context.getRequestHeaders().get(AUTHORIZATION_HEADER_NAME);

      if (jwtToken == null
          || (!jwtToken.startsWith("Bearer ") && !jwtToken.startsWith("bearer "))) {
        throw new AuthenticationException("Invalid Authorization token");
      }

      String token = getToken(jwtToken);

      Jws<Claims> claims =
          Jwts.parserBuilder()
              .setSigningKeyResolver(
                  new DataHubJwtSigningKeyResolver(
                      this.trustedIssuers, this.publicKey, this.algorithm))
              .build()
              .parseClaimsJws(token);

      final String userClaim = claims.getBody().get(userIdClaim, String.class);

      if (userClaim == null) {
        throw new AuthenticationException("Invalid or missing claim: " + userIdClaim);
      }

      return new Authentication(new Actor(ActorType.USER, userClaim), jwtToken);
    } catch (Exception e) {
      throw new AuthenticationException(e.getMessage());
    }
  }

  private String getToken(String jwtToken) {
    var tokenArray = jwtToken.split(" ");
    return tokenArray.length == 1 ? tokenArray[0] : tokenArray[1];
  }
}
