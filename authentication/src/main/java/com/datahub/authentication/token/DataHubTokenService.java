package com.datahub.authentication.token;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.Key;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;


/**
 * Service responsible for generating JWT tokens for use within DataHub.
 */
public class DataHubTokenService {

  private static final long DEFAULT_EXPIRES_IN_MS = 86400000L; // One day by default
  private static final List<String> SUPPORTED_ALGORITHMS = new ArrayList<>();

  static {
    SUPPORTED_ALGORITHMS.add("HS256"); // Only support HS256 today.
  }

  private final String signingKey;
  private final SignatureAlgorithm signingAlgorithm;
  private final String iss;

  public DataHubTokenService(
      final String signingKey,
      final String signingAlgorithm,
      final String iss
  ) {
    this.signingKey = signingKey;
    this.signingAlgorithm = validateAlgorithm(signingAlgorithm);
    this.iss = iss;
  }

  /**
   * Generates a JWT for an actor with a default expiration time.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   *
   */
  public String generateAccessToken(final DataHubAccessTokenType type, final String actor) {
    return generateAccessToken(type, actor, DEFAULT_EXPIRES_IN_MS);
  }

  /**
   * Generates a JWT for an actor with a specific duration in milliseconds.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   *
   */
  public String generateAccessToken(final DataHubAccessTokenType type, final String actor, final long expiresInMs) {
    Map<String, Object> claims = new HashMap<>();
    claims.put("type", type.toString());
    claims.put("actorUrn", actor);
    return generateAccessToken(actor, claims, expiresInMs);
  }

  /**
   * Validates a JWT issued by this service.
   * This method throws is the JWT fails to validate.
   */
  public DataHubAccessTokenClaims validateAccessToken(final String accessToken) {
    try {
      final Claims claims = (Claims) Jwts.parserBuilder()
          .setSigningKey(this.signingKey)
          .build()
          .parse(accessToken)
          .getBody();
      final String actorUrn = claims.get("actorUrn", String.class);
      if (actorUrn != null && actorUrn.length() > 0) {
        return new DataHubAccessTokenClaims(actorUrn);
      }
    } catch (Exception e) {
      throw new DataHubTokenException("Failed to validate DataHub token", e);
    }
    throw new DataHubTokenException("Failed to validate DataHub token: Found malformed or missing 'actor' claim.");
  }

  /**
   * Generates a JWT for a custom set of claims.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   */
  public String generateAccessToken(final String sub, final Map<String, Object> claims, final long expiresInMs) {
    final JwtBuilder builder = Jwts.builder()
      .addClaims(claims)
      .setExpiration(new Date(System.currentTimeMillis() + expiresInMs))
      .setId(UUID.randomUUID().toString())
      .setSubject(sub);
    if (this.iss != null) {
      builder.setIssuer(this.iss);
    }
    byte[] apiKeySecretBytes = Base64.getDecoder().decode(this.signingKey); // Key must be base64'd.
    final Key signingKey = new SecretKeySpec(apiKeySecretBytes, this.signingAlgorithm.getJcaName());
    return builder.signWith(signingKey, this.signingAlgorithm).compact();
  }

  private SignatureAlgorithm validateAlgorithm(final String algorithm) {
    if (!SUPPORTED_ALGORITHMS.contains(algorithm)) {
      throw new UnsupportedOperationException(
          String.format("Failed to create Token Service. Unsupported algorithm %s provided", algorithm));
    }
    return SignatureAlgorithm.valueOf(algorithm);
  }
}
