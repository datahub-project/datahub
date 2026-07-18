package com.linkedin.metadata.timeseries.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class TimeseriesFilterSqlBuilderTest {

  @Test
  public void stripKeywordSuffix_removesTrailingKeyword() {
    assertEquals(TimeseriesPgJsonPaths.stripKeywordSuffix("browsePaths.keyword"), "browsePaths");
    assertEquals(TimeseriesPgJsonPaths.stripKeywordSuffix("urn"), "urn");
  }

  @Test
  public void documentTextPathSql_singleSegment() {
    String sql = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql("timestampMillis");
    assertTrue(sql.contains("document->>'timestampMillis'"));
  }

  @Test
  public void documentTextPathSql_nested() {
    String sql = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql("userCounts.usageCount");
    assertTrue(sql.contains("document #>> "));
    assertTrue(sql.contains("'userCounts'"));
    assertTrue(sql.contains("'usageCount'"));
  }
}
