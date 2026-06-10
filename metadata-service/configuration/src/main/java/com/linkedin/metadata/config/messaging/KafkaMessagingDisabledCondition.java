package com.linkedin.metadata.config.messaging;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Inverse of {@link KafkaMessagingEnabledCondition}: matches when transport is not Kafka. */
public class KafkaMessagingDisabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String transport =
        context.getEnvironment().getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA);
    return !MessagingTransport.KAFKA.equalsIgnoreCase(transport);
  }
}
