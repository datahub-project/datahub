package com.linkedin.datahub.graphql.analytics.service.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TreeMap;
import org.testng.annotations.Test;

public class PostgresAnalyticsQueriesBucketTest {

  @Test
  public void expectedBucketKeys_fillsDailyGapsInRange() {
    // 2024-01-01 through 2024-01-04 (exclusive end)
    DateRange range = new DateRange();
    range.setStart(String.valueOf(Instant.parse("2024-01-01T00:00:00Z").toEpochMilli()));
    range.setEnd(String.valueOf(Instant.parse("2024-01-04T00:00:00Z").toEpochMilli()));

    List<String> keys = PostgresAnalyticsQueries.expectedBucketKeys(range, DateInterval.DAY);
    assertEquals(keys.size(), 3);
    assertEquals(keys.get(0), "2024-01-01T00:00:00Z");
    assertEquals(keys.get(1), "2024-01-02T00:00:00Z");
    assertEquals(keys.get(2), "2024-01-03T00:00:00Z");
  }

  @Test
  public void expectedBucketKeys_weekStartsMonday() {
    // Wednesday 2024-01-03 → week bucket Monday 2024-01-01
    Instant wed = Instant.parse("2024-01-03T15:00:00Z");
    Instant truncated = PostgresAnalyticsQueries.truncateBucketStart(wed, DateInterval.WEEK);
    assertEquals(truncated.atZone(ZoneOffset.UTC).getDayOfWeek().name(), "MONDAY");
    assertEquals(truncated, Instant.parse("2024-01-01T00:00:00Z"));
  }

  @Test
  public void bucketStartMsSql_truncatesInUtc() {
    String sql = PostgresAnalyticsQueries.bucketStartMsSql("day");
    assertTrue(sql.contains("AT TIME ZONE 'UTC'"));
    assertTrue(sql.contains("DATE_TRUNC('day'"));
  }

  @Test
  public void fillEmptyBuckets_insertsZerosForMissingKeys() {
    TreeMap<String, Integer> observed = new TreeMap<>();
    observed.put("2024-01-01T00:00:00Z", 5);
    observed.put("2024-01-03T00:00:00Z", 2);
    List<String> expected =
        List.of("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z");

    TreeMap<String, Integer> filled = PostgresAnalyticsQueries.fillEmptyBuckets(observed, expected);
    assertEquals(filled.get("2024-01-01T00:00:00Z").intValue(), 5);
    assertEquals(filled.get("2024-01-02T00:00:00Z").intValue(), 0);
    assertEquals(filled.get("2024-01-03T00:00:00Z").intValue(), 2);
    assertTrue(filled.containsKey("2024-01-02T00:00:00Z"));
  }
}
