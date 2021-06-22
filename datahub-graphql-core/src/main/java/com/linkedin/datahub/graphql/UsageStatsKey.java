package com.linkedin.datahub.graphql;

import com.linkedin.datahub.graphql.generated.WindowDuration;
import lombok.Data;
import lombok.RequiredArgsConstructor;


@Data
@RequiredArgsConstructor
public class UsageStatsKey {
  private String resource;
  private WindowDuration duration;
  private Long endTime;
  private Long startTime;
  private Integer maxBuckets;

  public UsageStatsKey(String resource, WindowDuration duration, Long endTime, Long startTime, Integer maxBuckets) {
    this.resource = resource;
    this.duration = duration;
    this.endTime = endTime;
    this.startTime = startTime;
    this.maxBuckets = maxBuckets;
  }
}
