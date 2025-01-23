package com.linkedin.datahub.upgrade.test;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.BatchTestEngine;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.batch.BatchTestEngineEnvConfig;
import com.linkedin.metadata.test.batch.BatchTestEngineExecConfig;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EvaluateTestsStep implements UpgradeStep {

  private final BatchTestEngine batchTestEngine;
  @VisibleForTesting @Getter @Setter private ExecutorService overrideExecutorService;

  public EvaluateTestsStep(
      @Nonnull OperationContext systemOpContext,
      @Nonnull EntityClient entityClient,
      @Nonnull EntitySearchService entitySearchService,
      @Nonnull TestEngine testEngine) {
    this.batchTestEngine =
        new BatchTestEngine(systemOpContext, entityClient, entitySearchService, testEngine);
  }

  @Override
  public String id() {
    return "EvaluateTests";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Starting to evaluate tests...");

        int batchSize =
            context
                .parsedArgs()
                .getOrDefault("batchSize", Optional.empty())
                .map(Integer::parseInt)
                .orElse(1000);
        int batchDelayMs =
            context
                .parsedArgs()
                .getOrDefault("batchDelayMs", Optional.empty())
                .map(Integer::parseInt)
                .orElse(250);

        BatchTestEngineExecConfig execConfig =
            BatchTestEngineExecConfig.builder()
                .runId(String.format("cron-%s", Instant.now()))
                .batchSize(batchSize)
                .batchDelayMs(batchDelayMs)
                .logger(s -> context.report().addLine(s))
                .build();

        batchTestEngine.execute(
            BatchTestEngineEnvConfig.fromEnvironment(), execConfig, overrideExecutorService);

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error(
            "Failed to complete test evaluation! Caught an exception while running the test. This may mean that the test run was incomplete.",
            e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }
}
