package com.linkedin.metadata.timeseries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class BatchWriteOperationsOptions {
  private int batchSize;
  private long timeoutSeconds;
}
