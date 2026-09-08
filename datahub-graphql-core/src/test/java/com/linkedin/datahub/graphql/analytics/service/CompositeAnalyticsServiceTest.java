package com.linkedin.datahub.graphql.analytics.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CompositeAnalyticsServiceTest {

  private AnalyticsService usage;
  private AnalyticsService entity;
  private CompositeAnalyticsService composite;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    usage = mock(AnalyticsService.class);
    entity = mock(AnalyticsService.class);
    opContext = mock(OperationContext.class);
    when(usage.getUsageIndexName(any())).thenReturn("datahub_usage_event");
    when(entity.getEntityIndexName(any(), eq(EntityType.DATASET))).thenReturn("datasetindex_v2");
    when(entity.getAllEntityIndexName(any())).thenReturn(".*index_v2");
    composite = new CompositeAnalyticsService(usage, entity);
  }

  @Test
  public void routesUsageIndexToUsageService() {
    DateRange range = new DateRange("1", "2");
    when(usage.getHighlights(any(), eq("datahub_usage_event"), any(), any(), any(), any()))
        .thenReturn(7);

    int result =
        composite.getHighlights(
            opContext,
            "datahub_usage_event",
            Optional.of(range),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Optional.empty());

    assertEquals(result, 7);
    verify(usage).getHighlights(any(), eq("datahub_usage_event"), any(), any(), any(), any());
    verify(entity, never()).getHighlights(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void routesEntityIndexToEntityService() {
    DateRange range = new DateRange("1", "2");
    when(entity.getTimeseriesChart(
            any(), eq("datasetindex_v2"), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(ImmutableList.of());

    composite.getTimeseriesChart(
        opContext,
        "datasetindex_v2",
        range,
        DateInterval.DAY,
        Optional.empty(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Optional.empty(),
        "timestamp");

    verify(entity)
        .getTimeseriesChart(
            any(), eq("datasetindex_v2"), any(), any(), any(), any(), any(), any(), any());
    verify(usage, never())
        .getTimeseriesChart(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void indexNameHelpersDelegateCorrectly() {
    assertEquals(composite.getUsageIndexName(opContext), "datahub_usage_event");
    assertEquals(composite.getEntityIndexName(opContext, EntityType.DATASET), "datasetindex_v2");
    assertEquals(composite.getAllEntityIndexName(opContext), ".*index_v2");
  }
}
