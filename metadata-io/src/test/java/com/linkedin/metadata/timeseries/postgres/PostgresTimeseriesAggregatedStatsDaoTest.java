package com.linkedin.metadata.timeseries.postgres;

import static org.testng.Assert.assertEquals;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.timeseries.AggregationType;
import java.math.BigDecimal;
import org.testng.annotations.Test;

public class PostgresTimeseriesAggregatedStatsDaoTest {

  @Test
  public void formatMetricCell_sumIntegral_emitsLongStringWithoutDecimal() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            6.0d, AggregationType.SUM, DataSchema.Type.LONG),
        "6");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            new BigDecimal("6.0"), AggregationType.SUM, DataSchema.Type.INT),
        "6");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            650.0d, AggregationType.SUM, DataSchema.Type.LONG),
        "650");
  }

  @Test
  public void formatMetricCell_sumFloating_keepsDecimalString() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            6.5d, AggregationType.SUM, DataSchema.Type.DOUBLE),
        "6.5");
  }

  @Test
  public void formatMetricCell_cardinality_emitsLongString() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            2L, AggregationType.CARDINALITY, DataSchema.Type.STRING),
        "2");
  }

  @Test
  public void formatMetricCell_null_isEsNullSentinel() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            null, AggregationType.SUM, DataSchema.Type.LONG),
        PostgresTimeseriesAggregatedStatsDao.ES_NULL_VALUE);
  }

  @Test
  public void documentTextPathSql_nestedCollectionPath() {
    String sql = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql("userCounts.user");
    assertEquals(sql, "document #>> ARRAY['userCounts','user']::text[]");
  }
}
