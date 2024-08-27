package com.linkedin.datahub.graphql;

import com.linkedin.usage.UsageTimeRange;
import lombok.Data;

@Data
public class UsageStatsKey {
  private String resource;
  private UsageTimeRange range;

  public UsageStatsKey(String resource, UsageTimeRange range) {
    this.resource = resource;
    this.range = range;
  }
}
