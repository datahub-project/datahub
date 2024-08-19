package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "tests" configuration block in application.yaml.on.yml */
@Data
public class TestsConfiguration {
  /** Whether tests are enabled */
  private boolean enabled;

  private TestsHookConfiguration hook;
  private int cacheRefreshIntervalSecs;
  private TestsBootstrapConfiguration bootstrap;
  private ElasticSearchTestExecutorConfiguration elasticSearchExecutor;
  private ForwardingActionConfiguration forwardingAction;
}
