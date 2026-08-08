package com.linkedin.gms.factory.analytics;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

public class PgAnalyticsRuntimePoolEnabledCondition extends AllNestedConditions {

  public PgAnalyticsRuntimePoolEnabledCondition() {
    super(ConfigurationPhase.REGISTER_BEAN);
  }

  @ConditionalOnProperty(name = "postgres.pgAnalytics.enabled", havingValue = "true")
  static class Enabled {}
}
