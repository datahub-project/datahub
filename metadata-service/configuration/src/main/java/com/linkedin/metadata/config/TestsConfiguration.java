package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/** POJO representing the "tests" configuration block in application.yaml.on.yml */
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class TestsConfiguration {
  /** Whether tests are enabled */
  private boolean enabled;

  private boolean jvmShutdownHookEnabled;

  private long futuresBatchSize;

  private TestsHookConfiguration hook;
  private int cacheRefreshDelayIntervalSecs;
  private int cacheRefreshIntervalSecs;
  private TestsBootstrapConfiguration bootstrap;
  private ElasticSearchTestExecutorConfiguration elasticSearchExecutor;
  private ForwardingActionConfiguration forwardingAction;
  private ActionsConfiguration actions;

  @Data
  public static class ActionsConfiguration {
    private int concurrency;
    private int queueSize;
    private int threadKeepAlive;
  }
}
