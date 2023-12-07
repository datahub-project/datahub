package com.linkedin.metadata.timeseries;

import lombok.Data;

@Data
public class BatchWriteOperationsOptions {
  private int batchSize;
  private long timeoutSeconds;
}
