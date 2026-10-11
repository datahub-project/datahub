package com.linkedin.metadata.kafka.config;

import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class PgQueueMessagingAndBatchMetadataChangeProposalProcessorCondition implements Condition {

  private final PgQueueMessagingTransportCondition pgQueue =
      new PgQueueMessagingTransportCondition();
  private final BatchMetadataChangeProposalProcessorCondition batch =
      new BatchMetadataChangeProposalProcessorCondition();

  @Override
  public boolean matches(
      @Nonnull ConditionContext context, @Nonnull AnnotatedTypeMetadata metadata) {
    return pgQueue.matches(context, metadata) && batch.matches(context, metadata);
  }
}
