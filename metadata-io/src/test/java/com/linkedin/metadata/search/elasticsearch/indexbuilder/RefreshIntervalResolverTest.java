package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.RefreshIntervals;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.testng.annotations.Test;

public class RefreshIntervalResolverTest {

  private static final OperationFingerprint OPERATION = OperationFingerprint.EMPTY;
  private static final IndexConvention CONVENTION =
      IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());

  private static RefreshIntervals services() {
    return RefreshIntervals.builder()
        .entitySeconds(3)
        .graphSeconds(4)
        .systemMetadataSeconds(5)
        .timeseriesSeconds(30)
        .usageSeconds(45)
        .entities("{\"dataset\": 10}")
        .aspects("{\"datasetProfile\": 60}")
        .build();
  }

  @Test
  public void testServiceFields() {
    RefreshIntervals intervals = services();
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals, CONVENTION, OPERATION, "system_metadata_service_v1"),
        5);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals, CONVENTION, OPERATION, "chart_operationaspect_v1"),
        30);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(intervals, CONVENTION, OPERATION, "chartindex_v2"),
        3);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals, CONVENTION, OPERATION, "graph_service_v1"),
        4);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals, CONVENTION, OPERATION, "datahub_usage_event"),
        45);
  }

  @Test
  public void testEntityOverrideCoversV2SemanticAndV3() {
    RefreshIntervals intervals = services();
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(intervals, CONVENTION, OPERATION, "datasetindex_v2"),
        10);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals, CONVENTION, OPERATION, "datasetindex_v2_semantic"),
        10);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(intervals, CONVENTION, OPERATION, "datasetindex_v3"),
        10);
  }

  @Test
  public void testAspectOverrideIsExact() {
    RefreshIntervals intervals = services();
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals, CONVENTION, OPERATION, "dataset_datasetprofileaspect_v1"),
        60);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals.toBuilder().aspects("{\"profile\": 7}").build(),
            CONVENTION,
            OPERATION,
            "dataset_datasetprofileaspect_v1"),
        30);
  }

  @Test
  public void testOverridesStayOnTheirService() {
    RefreshIntervals intervals = services();
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(
            intervals, CONVENTION, OPERATION, "dataset_datasetusagestatisticsaspect_v1"),
        30);
    assertEquals(
        RefreshIntervalResolver.resolveSeconds(intervals, CONVENTION, OPERATION, "chartindex_v2"),
        3);
  }

  @Test
  public void testMissingServiceFieldFails() {
    RefreshIntervals intervals = services().toBuilder().entitySeconds(null).build();
    try {
      RefreshIntervalResolver.resolveSeconds(intervals, CONVENTION, OPERATION, "chartindex_v2");
      throw new AssertionError("expected missing entitySeconds to fail");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("entitySeconds"));
    }
  }

  @Test
  public void testSameDuration() {
    assertTrue(RefreshIntervalResolver.sameDuration("30s", "30000ms"));
    assertTrue(RefreshIntervalResolver.sameDuration("1s", "1000ms"));
    assertFalse(RefreshIntervalResolver.sameDuration("30s", "3s"));
    assertTrue(ReindexConfig.settingValuesEqual(ESIndexBuilder.REFRESH_INTERVAL, "30s", "30000ms"));
    assertFalse(ReindexConfig.settingValuesEqual("number_of_replicas", "1", "1s"));
  }
}
