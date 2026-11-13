package com.datahub.authentication.token;

import jakarta.persistence.PersistenceException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Decides whether a failed token-store read is transient (the store could not be reached), as
 * opposed to a permanent problem with the token or its data. Only transient failures may be
 * answered with a retryable error; anything else (a malformed actor id, a corrupt aspect) must stay
 * an authentication failure, or the client would retry a request that can never succeed.
 */
final class TokenStoreFailures {

  private static final String HAZELCAST_PACKAGE = "com.hazelcast.";

  private TokenStoreFailures() {}

  /** True if any exception in the cause chain is a database, timeout or cache-cluster failure. */
  static boolean isStoreUnavailable(@Nullable final Throwable failure) {
    final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
      if (t instanceof PersistenceException
          || t instanceof SQLException
          || t instanceof TimeoutException
          // Hazelcast is optional at runtime, so match by package instead of by type.
          || t.getClass().getName().startsWith(HAZELCAST_PACKAGE)) {
        return true;
      }
    }
    return false;
  }
}
