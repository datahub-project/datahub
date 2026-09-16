package com.linkedin.metadata.queue;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** Non-locking row read from the pgQueue message log (External Events API). */
@Immutable
public record QueueLogPeekRow(
    @Nonnull QueueMessageHandle handle,
    int priority,
    @Nonnull byte[] payload,
    @Nonnull Optional<String> contentType,
    @Nonnull PgQueuePayloadCompression payloadCompression,
    @Nonnull List<QueueMessageHeader> headers,
    @Nonnull String routingKey) {}
