package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class ExecutorServiceConfig {
  @Builder.Default private int concurrency = 2;
  @Builder.Default private int queueSize = 100;
  @Builder.Default private int keepAlive = 60;
}
