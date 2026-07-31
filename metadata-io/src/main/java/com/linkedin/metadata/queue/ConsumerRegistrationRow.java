package com.linkedin.metadata.queue;

import java.time.Instant;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** A registered consumer group for aggressive retention tracking. */
@Immutable
public record ConsumerRegistrationRow(
    @Nonnull String consumerGroup,
    long topicId,
    @Nonnull Instant registeredAt,
    @Nonnull Instant lastHeartbeatAt) {}
