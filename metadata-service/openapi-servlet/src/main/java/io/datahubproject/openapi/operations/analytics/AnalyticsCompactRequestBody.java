package io.datahubproject.openapi.operations.analytics;

import com.fasterxml.jackson.annotation.JsonInclude;
import javax.annotation.Nullable;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnalyticsCompactRequestBody {
  @Nullable private Integer maxHoursToSeal;
  @Nullable private Integer maxDaysToCompact;
  @Nullable private Integer maxMonthsToCompact;
  @Nullable private Long maxWallClockMillis;

  /** Optional catch-up override; default scan horizon is 72 hours. */
  @Nullable private Integer hourLookbackHours;

  /** Optional catch-up override; default scan horizon is 14 days. */
  @Nullable private Integer dayLookbackDays;

  /** Optional catch-up override; default scan horizon is 3 months. */
  @Nullable private Integer monthLookbackMonths;
}
