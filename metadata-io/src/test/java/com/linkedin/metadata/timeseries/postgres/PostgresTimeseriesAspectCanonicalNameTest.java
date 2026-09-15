package com.linkedin.metadata.timeseries.postgres;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import java.util.List;
import org.testng.annotations.Test;

public class PostgresTimeseriesAspectCanonicalNameTest {

  @Test
  public void canonicalTimeseriesAspectName_resolvesElasticsearchLowercase() {
    AspectSpec usage = mock(AspectSpec.class);
    when(usage.getName()).thenReturn("datasetUsageStatistics");
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getAspectSpec("datasetusagestatistics")).thenReturn(null);
    when(spec.getAspectSpecs()).thenReturn(List.of(usage));
    assertEquals(
        PostgresTimeseriesAspectService.canonicalTimeseriesAspectName(
            spec, "datasetusagestatistics"),
        "datasetUsageStatistics");
  }

  @Test
  public void canonicalTimeseriesAspectName_keepsRegistryCasing() {
    AspectSpec usage = mock(AspectSpec.class);
    when(usage.getName()).thenReturn("queryUsageStatistics");
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getAspectSpec("queryUsageStatistics")).thenReturn(usage);
    assertEquals(
        PostgresTimeseriesAspectService.canonicalTimeseriesAspectName(spec, "queryUsageStatistics"),
        "queryUsageStatistics");
  }

  @Test
  public void canonicalTimeseriesAspectName_keepsUnknown() {
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getAspectSpec("notAnAspect")).thenReturn(null);
    when(spec.getAspectSpecs()).thenReturn(List.of());
    assertEquals(
        PostgresTimeseriesAspectService.canonicalTimeseriesAspectName(spec, "notAnAspect"),
        "notAnAspect");
  }
}
