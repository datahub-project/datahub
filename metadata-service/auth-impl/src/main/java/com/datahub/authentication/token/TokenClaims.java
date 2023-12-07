package com.datahub.authentication.token;

import com.datahub.authentication.ActorType;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Contains strongly-typed claims that appear in all DataHub granted access tokens. */
public class TokenClaims {

  public static final String TOKEN_VERSION_CLAIM_NAME = "version";
  public static final String TOKEN_TYPE_CLAIM_NAME = "type";
  public static final String ACTOR_TYPE_CLAIM_NAME = "actorType";
  public static final String ACTOR_ID_CLAIM_NAME = "actorId";
  public static final String EXPIRATION_CLAIM = "exp";

  /**
   * The type of the access token, e.g. a session token issued by the frontend or a personal access
   * token generated for programmatic use.
   */
  private final TokenVersion tokenVersion;

  /**
   * The type of the access token, e.g. a session token issued by the frontend or a personal access
   * token generated for programmatic use.
   */
  private final TokenType tokenType;

  /**
   * The type of an authenticated DataHub actor.
   *
   * <p>E.g. "urn:li:corpuser:johnsmith" is of type USER.
   */
  private final ActorType actorType;

  /**
   * A unique identifier for an actor of a particular type.
   *
   * <p>E.g. "johnsmith" inside urn:li:corpuser:johnsmith.
   */
  private final String actorId;

  /** The expiration time in milliseconds if one exists, null otherwise. */
  private final Long expirationInMs;

  public TokenClaims(
      @Nonnull TokenVersion tokenVersion,
      @Nonnull TokenType tokenType,
      @Nonnull final ActorType actorType,
      @Nonnull final String actorId,
      @Nullable Long expirationInMs) {
    Objects.requireNonNull(tokenVersion);
    Objects.requireNonNull(tokenType);
    Objects.requireNonNull(actorType);
    Objects.requireNonNull(actorId);
    this.tokenVersion = tokenVersion;
    this.tokenType = tokenType;
    this.actorType = actorType;
    this.actorId = actorId;
    this.expirationInMs = expirationInMs;
  }

  /** Returns the version of the access token */
  public TokenVersion getTokenVersion() {
    return this.tokenVersion;
  }

  /** Returns the type of an authenticated DataHub actor. */
  public TokenType getTokenType() {
    return this.tokenType;
  }

  /** Returns the type of an authenticated DataHub actor. */
  public ActorType getActorType() {
    return this.actorType;
  }

  /** Returns the expiration time in milliseconds if one exists, null otherwise. */
  public Long getExpirationInMs() {
    return this.expirationInMs;
  }

  /** Returns a unique id associated with a DataHub actor of a particular type. */
  public String getActorId() {
    return this.actorId;
  }

  /** Returns the claims in the DataHub Access token as a map. */
  public Map<String, Object> asMap() {
    return ImmutableMap.of(
        TOKEN_VERSION_CLAIM_NAME, this.tokenVersion.numericValue,
        TOKEN_TYPE_CLAIM_NAME, this.tokenType.toString(),
        ACTOR_TYPE_CLAIM_NAME, this.actorType.toString(),
        ACTOR_ID_CLAIM_NAME, this.actorId,
        EXPIRATION_CLAIM, Optional.ofNullable(this.expirationInMs));
  }
}
