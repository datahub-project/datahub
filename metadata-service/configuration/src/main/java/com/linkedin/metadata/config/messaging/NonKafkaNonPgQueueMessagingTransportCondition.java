package com.linkedin.metadata.config.messaging;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when {@link MessagingTransport#PROPERTY} is set to something other than Kafka or pgQueue
 * (for example a future transport). Used for beans that should apply only when neither Kafka nor
 * pgQueue metadata messaging is active.
 */
public class NonKafkaNonPgQueueMessagingTransportCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String transport =
        context.getEnvironment().getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA);
    return !MessagingTransport.KAFKA.equalsIgnoreCase(transport)
        && !MessagingTransport.PGQUEUE.equalsIgnoreCase(transport);
  }
}
