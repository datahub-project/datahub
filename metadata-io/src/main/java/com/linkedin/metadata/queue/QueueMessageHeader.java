package com.linkedin.metadata.queue;

import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nonnull;

/** Kafka-style record header: UTF-8 key and opaque value bytes. */
public record QueueMessageHeader(@Nonnull String key, @Nonnull byte[] value) {
  public QueueMessageHeader {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    value = Arrays.copyOf(value, value.length);
  }
}
