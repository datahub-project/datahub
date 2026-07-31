package com.linkedin.metadata.queue;

import javax.annotation.concurrent.Immutable;

/** A single priority band with an inclusive range and a relative weight for fair queuing. */
@Immutable
public record PriorityBand(int minPriority, int maxPriority, int weight) {

  public PriorityBand {
    if (minPriority < QueueTopicMetadata.MIN_PRIORITY
        || minPriority > QueueTopicMetadata.MAX_PRIORITY) {
      throw new IllegalArgumentException("minPriority " + minPriority + " out of range [0, 9]");
    }
    if (maxPriority < QueueTopicMetadata.MIN_PRIORITY
        || maxPriority > QueueTopicMetadata.MAX_PRIORITY) {
      throw new IllegalArgumentException("maxPriority " + maxPriority + " out of range [0, 9]");
    }
    if (minPriority > maxPriority) {
      throw new IllegalArgumentException(
          "minPriority " + minPriority + " > maxPriority " + maxPriority);
    }
    if (weight <= 0) {
      throw new IllegalArgumentException("weight must be positive, got " + weight);
    }
  }

  public boolean contains(int priority) {
    return priority >= minPriority && priority <= maxPriority;
  }
}
