package com.linkedin.datahub.graphql;

import com.linkedin.datahub.graphql.generated.TimeRange;
import lombok.Data;

@Data
public class TimeSeriesAspectArgs {
  private String aspectName;
  private String urn;
  private Long count;
  private TimeRange timeRange;

  public TimeSeriesAspectArgs(String urn, String aspectName, Long count, TimeRange timeRange) {
    this.urn = urn;
    this.aspectName = aspectName;
    this.count = count;
    this.timeRange = timeRange;
  }
}
