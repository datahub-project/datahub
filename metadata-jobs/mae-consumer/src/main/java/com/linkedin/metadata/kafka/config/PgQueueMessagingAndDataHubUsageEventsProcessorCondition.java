package com.linkedin.metadata.kafka.config;

import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class PgQueueMessagingAndDataHubUsageEventsProcessorCondition implements Condition {

  private final PgQueueMessagingTransportCondition pgQueue =
      new PgQueueMessagingTransportCondition();
  private final DataHubUsageEventsProcessorCondition usage =
      new DataHubUsageEventsProcessorCondition();

  @Override
  public boolean matches(
      @Nonnull ConditionContext context, @Nonnull AnnotatedTypeMetadata metadata) {
    return pgQueue.matches(context, metadata) && usage.matches(context, metadata);
  }
}
