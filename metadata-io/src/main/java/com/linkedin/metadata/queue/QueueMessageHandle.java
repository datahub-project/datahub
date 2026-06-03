package com.linkedin.metadata.queue;

import java.time.Instant;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** Primary key and routing fields required for acks and visibility extension. */
@Immutable
public record QueueMessageHandle(
    long id, @Nonnull Instant enqueuedAt, long topicId, int partitionId, long enqueueSeq) {}
