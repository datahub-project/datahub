package com.linkedin.metadata.kafka.config;

import com.linkedin.metadata.config.messaging.MessagingTransport;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * CDC processing requires Kafka transport since it consumes Debezium CDC events from a Kafka topic.
 */
public class CDCProcessorCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, @Nonnull AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    String transport = env.getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA);
    return MessagingTransport.KAFKA.equalsIgnoreCase(transport)
        && "true".equals(env.getProperty("mclProcessing.cdcSource.enabled", "false"))
        && "true".equals(env.getProperty("MCE_CONSUMER_ENABLED"));
  }
}
