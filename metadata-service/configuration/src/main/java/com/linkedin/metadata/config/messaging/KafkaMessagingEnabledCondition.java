package com.linkedin.metadata.config.messaging;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class KafkaMessagingEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String transport =
        context.getEnvironment().getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA);
    return MessagingTransport.KAFKA.equalsIgnoreCase(transport);
  }
}
