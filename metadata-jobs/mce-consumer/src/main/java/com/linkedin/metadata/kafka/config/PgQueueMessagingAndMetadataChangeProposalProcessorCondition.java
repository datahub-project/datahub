package com.linkedin.metadata.kafka.config;

import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Enables pgQueue MCP poller beans only when transport is pgQueue and MCP consumer is active. */
public class PgQueueMessagingAndMetadataChangeProposalProcessorCondition implements Condition {

  private final PgQueueMessagingTransportCondition pgQueue =
      new PgQueueMessagingTransportCondition();
  private final MetadataChangeProposalProcessorCondition mcp =
      new MetadataChangeProposalProcessorCondition();

  @Override
  public boolean matches(
      @Nonnull ConditionContext context, @Nonnull AnnotatedTypeMetadata metadata) {
    return pgQueue.matches(context, metadata) && mcp.matches(context, metadata);
  }
}
