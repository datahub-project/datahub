package com.linkedin.metadata.test.batch;

import java.util.function.Consumer;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BatchTestEngineExecConfig {
  private String runId;
  private int batchSize;
  private int batchDelayMs;
  private Consumer<String> logger;
}
