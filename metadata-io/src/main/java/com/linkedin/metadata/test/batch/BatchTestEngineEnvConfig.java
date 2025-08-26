package com.linkedin.metadata.test.batch;

import com.linkedin.metadata.test.TestEngine;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BatchTestEngineEnvConfig {
  public static final String ELASTIC_TIMEOUT_ENV_NAME = "METADATA_TEST_ELASTIC_TIMEOUT";
  public static final String EXECUTOR_POOL_SIZE = "EXECUTOR_POOL_SIZE";
  public static final String EXECUTOR_QUEUE_SIZE = "EXECUTOR_QUEUE_SIZE";
  public static final String EVALUATION_MODE = "EVALUATION_MODE";

  public static BatchTestEngineEnvConfig fromEnvironment() {
    String envEvalMode = System.getenv(EVALUATION_MODE);
    TestEngine.EvaluationMode evaluationMode =
        TestEngine.EvaluationMode.getEvaluationMode(envEvalMode);

    String pitKeepAlive = System.getenv().getOrDefault(ELASTIC_TIMEOUT_ENV_NAME, "15m");

    int numThreads =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(
                    EXECUTOR_POOL_SIZE,
                    String.valueOf(Runtime.getRuntime().availableProcessors() + 1)));
    int queueSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(
                    EXECUTOR_QUEUE_SIZE,
                    String.valueOf((Runtime.getRuntime().availableProcessors() + 1) * 2)));

    return BatchTestEngineEnvConfig.builder()
        .evaluationMode(evaluationMode)
        .executorNumThreads(numThreads)
        .executorQueueSize(queueSize)
        .elasticsearchPitKeepAlive(pitKeepAlive)
        .build();
  }

  @Nullable private String elasticsearchPitKeepAlive;
  @Nonnull private TestEngine.EvaluationMode evaluationMode;
  private int executorNumThreads;
  private int executorQueueSize;
}
