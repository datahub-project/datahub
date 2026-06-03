package com.linkedin.metadata.queue;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** A locked, unacknowledged queue message row. */
@Immutable
public record QueueReceivedMessage(
    @Nonnull QueueMessageHandle handle,
    int priority,
    @Nonnull byte[] payload,
    @Nonnull Optional<String> contentType,
    @Nonnull PgQueuePayloadCompression payloadCompression,
    @Nonnull List<QueueMessageHeader> headers,
    @Nonnull String routingKey,
    @Nonnull String lockOwner) {}
