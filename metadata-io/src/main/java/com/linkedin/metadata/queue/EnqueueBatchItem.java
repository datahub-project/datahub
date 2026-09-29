package com.linkedin.metadata.queue;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public record EnqueueBatchItem(
    @Nonnull String topicName,
    @Nonnull String routingKey,
    int priority,
    @Nonnull byte[] payload,
    @Nonnull Optional<String> contentType,
    @Nonnull List<QueueMessageHeader> headers,
    @Nonnull PgQueuePayloadCompression payloadCompression) {}
