package com.datahub.authentication.authenticator;

import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.plugins.auth.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorContext;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.authentication.AuthenticationConstants.*;


/**
 * This Authenticator verifies third party token and extract custom claim for userId.
 * Supported algorithm at this moment RS256
 */
@Slf4j
public class DatahubJwtTokenAuthenticator implements Authenticator {


  /**
   *  Custom claim name, configured to extract claim from token and set as user Id
   * **/
  private String idClaim;

  /**
   *  List of trusted issuers
   * **/
  private LinkedHashMap<String, String> trustedIssuers;

  @Override
  public void init(@Nonnull final Map<String, Object> config, @Nullable final AuthenticatorContext context) {
    Objects.requireNonNull(config, "Config parameter cannot be null");
    this.idClaim = Objects.requireNonNull((String) config.get("idClaim"), "Missing required config Claim Name");

    this.trustedIssuers = Objects.requireNonNull((LinkedHashMap<String, String>) config.get("trustedIssuers"),
        "Missing required config trusted issuers");
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest context) throws AuthenticationException {
    Objects.requireNonNull(context);

    try {
      String jwtToken = context.getRequestHeaders().get(AUTHORIZATION_HEADER_NAME);
      if (jwtToken == null) {
        throw new AuthenticationException("Invalid Authorization token");
      }

      String token = getToken(jwtToken);
      // Decode JWT token
      final DecodedJWT jwt = JWT.decode(token);

      // Stop validation if issuer is missing from valid issuers list
      if (!trustedIssuers.containsValue(jwt.getIssuer())) {
        throw new AuthenticationException("Invalid issuer");
      }

      // verify JWT token
      verifyToken(jwt);

      // Extract claim
      Claim claim = jwt.getClaim(this.idClaim);
      if (claim.isMissing() || claim.isNull()) {
        throw new AuthenticationException("Invalid or missing claim");
      }

      return new Authentication(
          new Actor(ActorType.USER, claim.asString()), jwtToken
      );

    } catch (Exception e) {
      throw new AuthenticationException(e.getMessage());
    }
  }

  private void verifyToken(DecodedJWT jwt) throws AuthenticationException {
    try {
      RSAPublicKey publicKey = getPublicKey(jwt);
      Algorithm algorithm = Algorithm.RSA256(publicKey, null);
      JWTVerifier verifier = JWT.require(algorithm)
          .withIssuer(jwt.getIssuer())
          .build();

      verifier.verify(jwt);
    } catch (Exception e) {
      throw new AuthenticationException(e.getMessage());
    }
  }

  public RSAPublicKey getPublicKey(DecodedJWT token) throws JwkException, MalformedURLException {

    final String url = token.getIssuer() + "/protocol/openid-connect/certs";
    JwkProvider provider = new UrlJwkProvider(new URL(url));
    return (RSAPublicKey) provider.get(token.getKeyId()).getPublicKey();
  }

  private String getToken(String jwtToken) {
    var tokenArray = jwtToken.split(" ");
    return tokenArray.length == 1 ? tokenArray[0] : tokenArray[1];
  }

}