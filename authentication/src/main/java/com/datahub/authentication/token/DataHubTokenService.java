package com.datahub.authentication.token;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.Key;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;


/**
 * Service responsible for generating JWT tokens for use within DataHub.
 */
public class DataHubTokenService {

  private static final List<String> SUPPORTED_ALGORITHMS = new ArrayList<>();

  static {
    SUPPORTED_ALGORITHMS.add("HS256"); // Only support HS256 today.
  }

  private final String signingKey;
  private final SignatureAlgorithm signingAlgorithm;
  private final long expiresInMs;
  private final String iss;

  public DataHubTokenService(
      final String signingKey,
      final String signingAlgorithm,
      final long expiresInMs,
      final String iss
  ) {
    this.signingKey = signingKey;
    this.signingAlgorithm = validateAlgorithm(signingAlgorithm);
    this.expiresInMs = expiresInMs;
    this.iss = iss;
  }

  /**
   * Generates a JWT for an actor with a specific urn.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   */
  private String generateToken(final String actorUrn) {
    Map<String, Object> claims = new HashMap<>();
    claims.put("urn", actorUrn);
    return generateToken(actorUrn, claims);
  }

  /**
   * Generates a JWT for a custom set of claims.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   */
  private String generateToken(final String sub, final Map<String, Object> claims) {
    final JwtBuilder builder = Jwts.builder()
      .addClaims(claims)
      .setExpiration(new Date(System.currentTimeMillis() + this.expiresInMs))
      .setId(UUID.randomUUID().toString())
      .setSubject(sub);
    if (this.iss != null) {
      builder.setIssuer(this.iss);
    }
    byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(this.signingKey); // Key must be base64'd.
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
