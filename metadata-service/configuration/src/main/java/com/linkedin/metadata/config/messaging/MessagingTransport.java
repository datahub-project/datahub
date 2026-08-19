package com.linkedin.metadata.config.messaging;

/** Values for {@link KafkaMessagingEnabledCondition#PROPERTY}. */
public final class MessagingTransport {

  private MessagingTransport() {}

  public static final String PROPERTY = "datahub.messaging.transport";

  public static final String KAFKA = "kafka";
  public static final String PGQUEUE = "pgqueue";
}
