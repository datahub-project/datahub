package com.linkedin.metadata.kafka.config;

import javax.annotation.Nonnull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class MetadataChangeProposalProcessorCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, @Nonnull AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    return ("true".equals(env.getProperty("MCE_CONSUMER_ENABLED"))
            || "true".equals(env.getProperty("MCP_CONSUMER_ENABLED")))
        && !Boolean.parseBoolean(
            env.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"));
  }
}
