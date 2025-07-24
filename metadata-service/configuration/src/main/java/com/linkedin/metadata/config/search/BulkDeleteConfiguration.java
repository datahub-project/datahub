package com.linkedin.metadata.config.search;

import com.linkedin.metadata.utils.ParseUtils;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class BulkDeleteConfiguration {
  private int batchSize;
  private String slices;
  private int pollInterval;
  private String pollIntervalUnit;
  private int timeout;
  private String timeoutUnit;
  private int numRetries;

  public Duration getPollDuration() {
    return ParseUtils.parseDuration(pollInterval, pollIntervalUnit);
  }

  public Duration getTimeoutDuration() {
    return ParseUtils.parseDuration(timeout, timeoutUnit);
  }
}
