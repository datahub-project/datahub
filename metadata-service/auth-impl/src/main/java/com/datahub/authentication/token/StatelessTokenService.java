package com.datahub.authentication.token;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.datahub.authentication.token.TokenClaims.*;


/**
 * Service responsible for generating JWT tokens for use within DataHub in stateless way.
 * This service is responsible only for generating tokens, it will not do anything else with them.
 */
public class StatelessTokenService {

  protected static final long DEFAULT_EXPIRES_IN_MS = 86400000L; // One day by default
  private static final List<String> SUPPORTED_ALGORITHMS = new ArrayList<>();

  static {
    SUPPORTED_ALGORITHMS.add("HS256"); // Only support HS256 today.
  }

  private final String signingKey;
  private final SignatureAlgorithm signingAlgorithm;
  private final String iss;

  public StatelessTokenService(
      @Nonnull final String signingKey,
      @Nonnull final String signingAlgorithm
  ) {
    this(signingKey, signingAlgorithm, null);
  }

  public StatelessTokenService(
      @Nonnull final String signingKey,
      @Nonnull final String signingAlgorithm,
      @Nullable final String iss
  ) {
    this.signingKey = Objects.requireNonNull(signingKey);
    this.signingAlgorithm = validateAlgorithm(Objects.requireNonNull(signingAlgorithm));
    this.iss = iss;
  }

  /**
   * Generates a JWT for an actor with a default expiration time.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   *
   */
  public String generateAccessToken(@Nonnull final TokenType type, @Nonnull final Actor actor) {
    return generateAccessToken(type, actor, DEFAULT_EXPIRES_IN_MS);
  }

  /**
   * Generates a JWT for an actor with a specific duration in milliseconds.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   *
   */
  @Nonnull
  public String generateAccessToken(
      @Nonnull final TokenType type,
      @Nonnull final Actor actor,
      final long expiresInMs) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(actor);
    Map<String, Object> claims = new HashMap<>();
    claims.put(TOKEN_VERSION_CLAIM_NAME, String.valueOf(TokenVersion.ONE.numericValue)); // Hardcode version 1 for now.
    claims.put(TOKEN_TYPE_CLAIM_NAME, type.toString());
    claims.put(ACTOR_TYPE_CLAIM_NAME, actor.getType());
    claims.put(ACTOR_ID_CLAIM_NAME, actor.getId());
    return generateAccessToken(actor.getId(), claims, expiresInMs);
  }

  /**
   * Generates a JWT for a custom set of claims.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   */
  @Nonnull
  public String generateAccessToken(@Nonnull final String sub, @Nonnull final Map<String, Object> claims, final long expiresInMs) {
    Objects.requireNonNull(sub);
    Objects.requireNonNull(claims);
    final JwtBuilder builder = Jwts.builder()
      .addClaims(claims)
      .setExpiration(new Date(System.currentTimeMillis() + expiresInMs))
      .setId(UUID.randomUUID().toString())
      .setSubject(sub);
    if (this.iss != null) {
      builder.setIssuer(this.iss);
    }
    byte [] apiKeySecretBytes = this.signingKey.getBytes(StandardCharsets.UTF_8);
    final Key signingKey = new SecretKeySpec(apiKeySecretBytes, this.signingAlgorithm.getJcaName());
      return builder.signWith(signingKey, this.signingAlgorithm).compact();
  }

  /**
   * Validates a JWT issued by this service.
   *
   * Throws an {@link TokenException} in the case that the token cannot be verified.
   */
  @Nonnull
  public TokenClaims validateAccessToken(@Nonnull final String accessToken) throws TokenException {
    Objects.requireNonNull(accessToken);
    try {
      byte [] apiKeySecretBytes = this.signingKey.getBytes(StandardCharsets.UTF_8);
      final String base64Key = Base64.getEncoder().encodeToString(apiKeySecretBytes);
      final Claims claims = (Claims) Jwts.parserBuilder()
          .setSigningKey(base64Key)
          .build()
          .parse(accessToken)
          .getBody();
      final String tokenVersion = claims.get(TOKEN_VERSION_CLAIM_NAME, String.class);
      final String tokenType = claims.get(TOKEN_TYPE_CLAIM_NAME, String.class);
      final String actorId = claims.get(ACTOR_ID_CLAIM_NAME, String.class);
      final String actorType = claims.get(ACTOR_TYPE_CLAIM_NAME, String.class);
      if (tokenType != null && actorId != null && actorType != null) {
          return new TokenClaims(
              TokenVersion.fromNumericStringValue(tokenVersion),
              TokenType.valueOf(tokenType),
              ActorType.valueOf(actorType),
              actorId,
              claims.getExpiration().getTime());
      }
    } catch (io.jsonwebtoken.ExpiredJwtException e) {
      throw new TokenExpiredException("Failed to validate DataHub token. Token has expired.", e);
    } catch (Exception e) {
      throw new TokenException("Failed to validate DataHub token", e);
    }
    throw new TokenException("Failed to validate DataHub token: Found malformed or missing 'actor' claim.");
  }

  private SignatureAlgorithm validateAlgorithm(final String algorithm) {
    if (!SUPPORTED_ALGORITHMS.contains(algorithm)) {
      throw new UnsupportedOperationException(
          String.format("Failed to create Token Service. Unsupported algorithm %s provided", algorithm));
    }
    return SignatureAlgorithm.valueOf(algorithm);
  }
}
