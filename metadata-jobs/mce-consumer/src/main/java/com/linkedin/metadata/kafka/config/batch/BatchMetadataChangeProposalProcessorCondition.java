/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.config.batch;

import javax.annotation.Nonnull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class BatchMetadataChangeProposalProcessorCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, @Nonnull AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    return ("true".equals(env.getProperty("MCE_CONSUMER_ENABLED"))
            || "true".equals(env.getProperty("MCP_CONSUMER_ENABLED")))
        && Boolean.parseBoolean(
            env.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"));
  }
}
