package com.linkedin.metadata.pgqueue;

import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Contributes one or more {@link PgQueuePollerRegistration} beans (merged at startup). */
@FunctionalInterface
public interface PgQueuePollerSource {

  @Nonnull
  Stream<PgQueuePollerRegistration> registrations();
}
