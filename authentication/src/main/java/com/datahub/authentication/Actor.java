package com.datahub.authentication;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Represents a unique DataHub actor (i.e. principal). Defining characteristics of all DataHub Actors includes a
 *
 *  a) Actor Type: A specific type of actor, e.g. CORP_USER or SERVICE_USER.
 *  b) Actor Id: A unique id for the actor.
 *
 * These pieces of information are in turn used to construct an Entity Urn, which can be used as a primary key to fetch & update specific information
 * about the actor.
 */
public class Actor {

  private final ActorType type;
  private final String id;

  public Actor(@Nonnull final ActorType type, @Nonnull final String id) {
    this.type = Objects.requireNonNull(type);
    this.id = Objects.requireNonNull(id);
  }

  /**
   * Returns the {@link ActorType} associated with a DataHub actor.
   */
  @Nonnull
  public ActorType getType() {
    return this.type;
  }

  /**
   * Returns the unique id associated with a DataHub actor.
   */
  @Nonnull
  public String getId() {
    return this.id;
  }

  /**
   * Returns the string-ified urn rendering of the authenticated actor.
   */
  @Nonnull
  public String toUrnStr() {
    switch (getType()) {
      case CORP_USER:
        return String.format("urn:li:corpuser:%s", getId());
      default:
        throw new IllegalArgumentException(String.format("Unrecognized ActorType %s provided", getType()));
    }
  }
}
