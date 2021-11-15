package com.datahub.authentication.token;

import com.datahub.authentication.ActorType;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Contains strongly-typed claims that appear in all DataHub granted access tokens.
 */
public class TokenClaims {

  /**
   * The type of the access token, e.g. a session token issued by the frontend or a personal access token
   * generated for programmatic use.
   */
  private final TokenVersion tokenVersion;

  /**
   * The type of the access token, e.g. a session token issued by the frontend or a personal access token
   * generated for programmatic use.
   */
  private final TokenType tokenType;

  /**
   * The type of an authenticated DataHub actor.
   *
   * E.g. "urn:li:corpuser:johnsmith" is of type USER.
   */
  private final ActorType actorType;

  /**
   * A unique identifier for an actor of a particular type.
   *
   * E.g. "johnsmith" inside urn:li:corpuser:johnsmith.
   */
  private final String actorId;

  public TokenClaims(
      @Nonnull TokenVersion tokenVersion,
      @Nonnull TokenType tokenType,
      @Nonnull final ActorType actorType,
      @Nonnull final String actorId) {
    Objects.requireNonNull(tokenVersion);
    Objects.requireNonNull(tokenType);
    Objects.requireNonNull(actorType);
    Objects.requireNonNull(actorId);
    this.tokenVersion = tokenVersion;
    this.tokenType = tokenType;
    this.actorType = actorType;
    this.actorId = actorId;
  }

  /**
   * Returns the version of the access token
   */
  public TokenVersion getTokenVersion() {
    return this.tokenVersion;
  }

  /**
   * Returns the type of an authenticated DataHub actor.
   */
  public TokenType getTokenType() {
    return this.tokenType;
  }

  /**
   * Returns the type of an authenticated DataHub actor.
   */
  public ActorType getActorType() {
    return this.actorType;
  }

  /**
   * Returns a unique id associated with a DataHub actor of a particular type.
   */
  public String getActorId() {
    return this.actorId;
  }

}
