package com.linkedin.metadata.health;

import javax.annotation.Nonnull;

/** Result of probing the primary metadata entity store (relational via Ebean or Cassandra). */
public final class DatastoreHealthStatus {

  private final boolean healthy;
  private final String message;

  private DatastoreHealthStatus(boolean healthy, String message) {
    this.healthy = healthy;
    this.message = message;
  }

  public static DatastoreHealthStatus ok(@Nonnull String message) {
    return new DatastoreHealthStatus(true, message);
  }

  public static DatastoreHealthStatus unhealthy(@Nonnull String message) {
    return new DatastoreHealthStatus(false, message);
  }

  public boolean isHealthy() {
    return healthy;
  }

  @Nonnull
  public String getMessage() {
    return message;
  }
}
