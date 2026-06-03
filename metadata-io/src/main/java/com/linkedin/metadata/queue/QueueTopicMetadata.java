package com.linkedin.metadata.queue;

import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
public record QueueTopicMetadata(
    long id, int partitionCount, Optional<Integer> defaultContentTypeId) {

  public static final int MIN_PRIORITY = 0;
  public static final int MAX_PRIORITY = 9;
  public static final int DEFAULT_PRIORITY = 5;

  public static void validatePriority(int priority) {
    if (priority < MIN_PRIORITY || priority > MAX_PRIORITY) {
      throw new IllegalArgumentException(
          "priority " + priority + " out of range [" + MIN_PRIORITY + ", " + MAX_PRIORITY + "]");
    }
  }
}
