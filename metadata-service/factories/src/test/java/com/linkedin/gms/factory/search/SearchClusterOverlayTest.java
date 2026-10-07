package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import java.util.Map;
import org.testng.annotations.Test;

public class SearchClusterOverlayTest {

  @Test
  public void testKnownBulkProcessorKeyIsApplied() {
    BulkProcessorConfiguration defaults =
        BulkProcessorConfiguration.builder().requestsLimit(100).flushPeriod(5).build();
    BulkProcessorConfiguration result =
        SearchClusterOverlay.apply(
            new ObjectMapper(), defaults, Map.of("requestsLimit", 500, "flushPeriod", 9));
    assertEquals(result.getRequestsLimit(), 500);
    assertEquals(result.getFlushPeriod(), 9);
  }

  @Test
  public void testUnknownBulkProcessorKeyIsIgnored() {
    BulkProcessorConfiguration defaults =
        BulkProcessorConfiguration.builder().requestsLimit(100).flushPeriod(5).build();
    BulkProcessorConfiguration result =
        SearchClusterOverlay.apply(
            new ObjectMapper(),
            defaults,
            Map.of("requestLimit", 999, "requests-limit", 888, "requestsLimit", 500));
    assertEquals(result.getRequestsLimit(), 500);
    assertEquals(result.getFlushPeriod(), 5);
  }

  @Test
  public void testUnknownBuildIndicesKeyIsIgnored() {
    BuildIndicesConfiguration defaults =
        BuildIndicesConfiguration.builder().reindexBatchSize(1000).maxReindexHours(12).build();
    BuildIndicesConfiguration result =
        SearchClusterOverlay.apply(
            new ObjectMapper(),
            defaults,
            Map.of("reindexBatchSizez", 50, "reindexBatchSize", 2000));
    assertEquals(result.getReindexBatchSize(), Integer.valueOf(2000));
    assertEquals(result.getMaxReindexHours(), 12);
  }
}
