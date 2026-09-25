package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.config.hazelcast.HazelcastBootstrapProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Enables Spring scheduling for {@code RetentionDrainer} in ANY process that wires the retention
 * buffer (both {@code featureFlags.retentionBufferEnabled} and {@code
 * featureFlags.postCommitRetentionEnabled} true) — including standalone MCE consumers, which
 * otherwise carry no {@code @EnableScheduling}. So every ingesting pod (GMS or MCE) runs the
 * drainer; the shared drain lock still makes exactly one win per tick cluster-wide.
 *
 * <p>Gated by the same flags so no scheduling is added when the buffer is off. Harmless where
 * scheduling is already on (e.g. GMS via {@code ScheduledAnalyticsFactory}) —
 * {@code @EnableScheduling} only registers infrastructure and is idempotent.
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(
    name = {
      HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
      HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
    },
    havingValue = "true")
public class RetentionBufferSchedulingConfig {}
