package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class TimeseriesAspectServiceConfig {
  @Builder.Default private ExecutorServiceConfig query = ExecutorServiceConfig.builder().build();
}
