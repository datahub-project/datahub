package com.linkedin.metadata.config.shared;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ResultsLimitConfig {
  private int max;
  private int apiDefault;
  private boolean strict;
}
