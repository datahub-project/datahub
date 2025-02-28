package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class MetadataChangeProposalProcessorCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    return ("true".equals(env.getProperty("MCE_CONSUMER_ENABLED"))
            || "true".equals(env.getProperty("MCP_CONSUMER_ENABLED")))
        && !Boolean.parseBoolean(env.getProperty("MCP_CONSUMER_BATCH_ENABLED"));
  }
}
